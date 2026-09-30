package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Translates a subscription's {@code priority} field (0-255, a full octet --
 * see {@code NGSIConstants.NGSI_LD_PRIORITY_MIN/MAX}) into a small, documented
 * service class, following the same pattern as DiffServ: the codepoint space
 * is wide so the NGSI-LD model isn't boxed in, but
 * {@link PriorityNotificationDispatcher} only ever needs to reason about a
 * handful of per-class behaviors (PHBs) to stay analysable.
 *
 * <p>
 * The boundaries are configurable (variable number of classes, which the
 * 2/4/8-class sensitivity experiment relies on) via
 * {@code scorpio.subscription.notification.priority-class-boundaries}: an
 * ascending list of N-1 values for N classes. With the default
 * {@code 64,128,192} the 4 classes are:
 * <pre>
 *   class 0  [0-63]    best-effort  -- default priority (unmarked)
 *   class 1  [64-127]  routine
 *   class 2  [128-191] operational
 *   class 3  [192-255] critical
 * </pre>
 * Higher class = higher priority, the same convention as the {@code priority}
 * field itself.
 */
@ApplicationScoped
public class ServiceClassMapper {

	@ConfigProperty(name = "scorpio.subscription.notification.priority-class-boundaries", defaultValue = "64,128,192")
	String boundariesRaw;

	@ConfigProperty(name = "scorpio.subscription.notification.priority-class-names",
			defaultValue = "best-effort,routine,operational,critical")
	String namesRaw;

	private int[] boundaries;
	private String[] names;

	@PostConstruct
	void init() {
		boundaries = Arrays.stream(boundariesRaw.split(","))
				.map(String::trim)
				.filter(s -> !s.isEmpty())
				.mapToInt(Integer::parseInt)
				.toArray();
		for (int i = 0; i < boundaries.length; i++) {
			if (i > 0 && boundaries[i] <= boundaries[i - 1]) {
				throw new IllegalStateException(
						"scorpio.subscription.notification.priority-class-boundaries must be strictly "
								+ "ascending, got: " + boundariesRaw);
			}
			if (boundaries[i] < 1 || boundaries[i] > 255) {
				throw new IllegalStateException(
						"Each class boundary must be within [1,255], got: " + boundariesRaw);
			}
		}

		int numClasses = boundaries.length + 1;
		String[] given = Arrays.stream(namesRaw.split(",")).map(String::trim).filter(s -> !s.isEmpty())
				.toArray(String[]::new);
		if (given.length == numClasses) {
			names = given;
		} else {
			// The number of names doesn't match the number of classes (e.g.
			// when varying the boundaries for the sensitivity experiment
			// without adjusting the names at the same time) -- fall back to
			// generic labels rather than failing, since the names are only
			// ever used for logging/documentation.
			names = java.util.stream.IntStream.range(0, numClasses).mapToObj(i -> "class-" + i)
					.collect(Collectors.toList()).toArray(String[]::new);
		}
	}

	/** Number of configured service classes (boundaries.length + 1). */
	public int numClasses() {
		return boundaries.length + 1;
	}

	/**
	 * Service class of a raw priority (0-255). 0 = least prioritary class
	 * (best-effort with the default configuration), {@code numClasses()-1} =
	 * most prioritary.
	 */
	public int classOf(int priority) {
		int cls = 0;
		for (int boundary : boundaries) {
			if (priority >= boundary) {
				cls++;
			} else {
				break;
			}
		}
		return cls;
	}

	/** Human-readable label of a class, for logging/documentation. */
	public String nameOf(int serviceClass) {
		if (serviceClass < 0 || serviceClass >= names.length) {
			return "class-" + serviceClass;
		}
		return names[serviceClass];
	}
}
