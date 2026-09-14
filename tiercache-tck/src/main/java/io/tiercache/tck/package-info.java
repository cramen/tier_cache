/**
 * Public chaos-test harnesses of the TierCache TCK (Technology Compatibility
 * Kit): reusable drivers such as {@link io.tiercache.tck.StampedeHarness}
 * that encode the failure-mode scenarios (stampede, avalanche, penetration,
 * degradation) the compliance suite exercises against real Redis/Valkey
 * containers.
 *
 * <p>The scenario tests themselves live in the test sources of this module
 * and are published as a compliance-suite jar with the {@code tests}
 * classifier.
 *
 * @since 0.1.0
 */
package io.tiercache.tck;
