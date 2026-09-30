package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.UniEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Bounded, class-ordered dispatcher for outgoing subscription notifications.
 * Shared globally across all matching entity-update events (not one instance
 * per event), since Kafka messages are acknowledged before their
 * notifications finish sending, so several {@code checkSubscriptions} calls
 * can be in flight at once.
 *
 * <p>
 * The dispatcher does NOT schedule directly on the raw 0-255 {@code priority}
 * value. It only ever looks at the small, documented service class that
 * {@link ServiceClassMapper} derives from it (see that class for the
 * boundaries/PHB naming) -- the same separation DiffServ makes between a wide
 * codepoint space and a small set of per-hop behaviors. Two jobs in the same
 * class are indistinguishable to the scheduler and are served strictly FIFO;
 * the raw priority only ever decides which class a job lands in, never the
 * order within a class.
 *
 * <p>
 * When a free slot exists, a job runs immediately (no queueing overhead in
 * the common case). Once {@code maxConcurrency} jobs are in flight, further
 * jobs are held in a bounded queue and released in class order (highest class
 * first, ties broken by arrival order) as slots free up. If the queue is
 * full, the incoming job is admitted only if its class outranks the
 * lowest-class job already queued, which is then evicted and reported via
 * {@code onDropped}.
 */
@ApplicationScoped
public class PriorityNotificationDispatcher {

	private static final Logger logger = LoggerFactory.getLogger(PriorityNotificationDispatcher.class);

	@Inject
	ServiceClassMapper classMapper;

	@ConfigProperty(name = "scorpio.subscription.notification.max-concurrency", defaultValue = "50")
	int maxConcurrency;

	@ConfigProperty(name = "scorpio.subscription.notification.queue-capacity", defaultValue = "2000")
	int queueCapacity;

	// Negate serviceClass so the PriorityQueue (a min-heap) surfaces the
	// HIGHEST class first; ties within a class fall back to submission order
	// (FIFO), since sequence is assigned monotonically by sequenceGen.
	private static final Comparator<Job> ORDER = Comparator.<Job>comparingInt(j -> -j.serviceClass)
			.thenComparingLong(j -> j.sequence);

	private final Object lock = new Object();
	private final PriorityQueue<Job> queue = new PriorityQueue<>(ORDER);
	private final AtomicLong sequenceGen = new AtomicLong();
	private int active = 0;

	/**
	 * Submits a notification job for a subscription with the given raw
	 * {@code priority} (0-255); it is mapped to a service class internally.
	 * {@code action} performs the actual send and must resolve (never fail)
	 * when the job completes, exactly like {@code sendNotification} already
	 * does. {@code onDropped} is invoked instead of {@code action} if the job
	 * is discarded due to sustained saturation.
	 */
	public Uni<Void> submit(int priority, Supplier<Uni<Void>> action, Supplier<Uni<Void>> onDropped) {
		int serviceClass = classMapper.classOf(priority);
		return Uni.createFrom().<Void>emitter(emitter -> {
			Job job = new Job(priority, serviceClass, sequenceGen.incrementAndGet(), action, onDropped, emitter);
			Job toRun = null;
			Job toDrop = null;
			synchronized (lock) {
				if (active < maxConcurrency) {
					active++;
					toRun = job;
				} else if (queue.size() < queueCapacity) {
					queue.offer(job);
				} else {
					// Queue is full: only admit the new job if it outranks the
					// weakest job currently queued, evicting that one instead.
					// Otherwise the new job itself is the one dropped. This
					// swap-and-decide happens atomically under the lock so two
					// concurrent submit() calls can't both evict the same victim
					// or both believe they found room.
					Job lowest = findLowestClass();
					if (job.serviceClass > lowest.serviceClass) {
						queue.remove(lowest);
						queue.offer(job);
						toDrop = lowest;
					} else {
						toDrop = job;
					}
				}
			}
			if (toRun != null) {
				runJob(toRun);
			}
			if (toDrop != null) {
				dropJob(toDrop);
			}
		});
	}

	// PriorityQueue is a min-heap over ORDER, so it gives O(1)/O(log n) access
	// to the HIGHEST-ranked (head) element via peek()/poll(), but the element
	// we need here -- the weakest queued job, i.e. the eviction candidate -- can
	// sit anywhere among the leaves. There's no heap-native way to find it, so
	// this falls back to a linear scan. Fine in practice: it only runs when the
	// queue is already at capacity, and queueCapacity is expected to stay in
	// the low thousands.
	private Job findLowestClass() {
		Job lowest = null;
		for (Job candidate : queue) {
			if (lowest == null || ORDER.compare(candidate, lowest) > 0) {
				lowest = candidate;
			}
		}
		return lowest;
	}

	private void runJob(Job job) {
		job.action.get().subscribe().with(item -> {
			// Always complete the emitter once the job has actually run, on
			// both the success AND failure branch: the Uni returned by
			// submit() represents "this job was scheduled and executed", not
			// "the notification succeeded" -- exactly the same contract
			// sendNotification already honors elsewhere. A caller awaiting
			// that Uni must never hang just because a downstream subscriber
			// endpoint was unreachable.
			job.emitter.complete(null);
			onJobDone();
		}, failure -> {
			logger.error("Notification job failed unexpectedly", failure);
			job.emitter.complete(null);
			onJobDone();
		});
	}

	private void onJobDone() {
		Job next;
		synchronized (lock) {
			active--;
			// Pull the next job and re-increment active in the SAME
			// critical section as the decrement, so there's no window where
			// a concurrent submit() could observe a free slot that this
			// method is about to hand to a queued job anyway.
			next = queue.poll();
			if (next != null) {
				active++;
			}
		}
		if (next != null) {
			runJob(next);
		}
	}

	private void dropJob(Job job) {
		logger.warn(
				"Notification dispatch queue saturated (capacity={}), dropping job with priority {} (class {}/{})",
				queueCapacity, job.priority, classMapper.nameOf(job.serviceClass), classMapper.numClasses());
		// Same never-fail contract as runJob: onDropped is the caller's
		// "record this as a dropped notification" hook, and its own outcome
		// must not leak back into the emitter either way.
		job.onDropped.get().subscribe().with(item -> job.emitter.complete(null),
				failure -> job.emitter.complete(null));
	}

	private static final class Job {
		final int priority; // raw 0-255 value, kept only for logging -- never used for ordering
		final int serviceClass; // the only value ORDER actually schedules on
		final long sequence; // monotonic submission index, breaks ties within a class (FIFO)
		final Supplier<Uni<Void>> action;
		final Supplier<Uni<Void>> onDropped;
		final UniEmitter<? super Void> emitter;

		Job(int priority, int serviceClass, long sequence, Supplier<Uni<Void>> action, Supplier<Uni<Void>> onDropped,
				UniEmitter<? super Void> emitter) {
			this.priority = priority;
			this.serviceClass = serviceClass;
			this.sequence = sequence;
			this.action = action;
			this.onDropped = onDropped;
			this.emitter = emitter;
		}
	}
}
