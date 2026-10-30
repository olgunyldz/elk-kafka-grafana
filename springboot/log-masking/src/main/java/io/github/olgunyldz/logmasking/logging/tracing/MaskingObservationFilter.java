package io.github.olgunyldz.logmasking.logging.tracing;

import io.github.olgunyldz.logmasking.logging.PciLogger;
import io.github.olgunyldz.logmasking.logging.SensitiveDataMasker;
import io.micrometer.common.KeyValue;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationFilter;

/**
 * Masks card and personal data in the key values of every observation, before they become span
 * attributes (traces) or metric tags. Span attributes such as {@code http.url} carry what the client
 * sent, query string included, so without this a card number in a URL would be stored in Tempo.
 * <p>
 * Uses the same rules as the log masking ({@link PciLogger#masker()}, configured from
 * {@code logging.masking.*}). Spring Boot applies every {@link ObservationFilter} bean to the
 * observation registry; {@link ObservationMaskingAutoConfiguration} registers this one.
 */
public class MaskingObservationFilter implements ObservationFilter {

    @Override
    public Observation.Context map(Observation.Context context) {
        SensitiveDataMasker masker = PciLogger.masker();
        // KeyValues are immutable snapshots: adding a key value with the same key replaces it
        for (KeyValue kv : context.getLowCardinalityKeyValues()) {
            String masked = masker.mask(kv.getValue());
            if (!masked.equals(kv.getValue())) {
                context.addLowCardinalityKeyValue(KeyValue.of(kv.getKey(), masked));
            }
        }
        for (KeyValue kv : context.getHighCardinalityKeyValues()) {
            String masked = masker.mask(kv.getValue());
            if (!masked.equals(kv.getValue())) {
                context.addHighCardinalityKeyValue(KeyValue.of(kv.getKey(), masked));
            }
        }
        return context;
    }
}
