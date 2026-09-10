package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import static me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants.TX_CHAIN_CONFIRMATION_PROFILE;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformContractsUtils;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.context.annotation.Profile;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Profile(TX_CHAIN_CONFIRMATION_PROFILE)
public class HistoricalTxIdRepositoryAdapter {

    private final HistoricalWeatherTelemetryTxIdsJpaRepository jpaTxIdsRepository;

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void processTxIdIngestionPacket(@NonNull WeatherPacket packet) {
        String uniqueTxId = PlatformContractsUtils.getUniqueTxId(packet);
        jpaTxIdsRepository.insertOrRemoveByIngestionTxId(UUID.randomUUID(), uniqueTxId);
    }

    public void processTxIdsHistoryBatch(@NonNull WeatherMap weatherMap) {
        List<String> txIds = weatherMap.getTelemetryTransactionsIdList();
        if(txIds.isEmpty()) {
            throw new IllegalArgumentException("Telemetry transaction ID list cannot be empty for batch processing");
        }
        int batchSize = txIds.size();
        UUID[] generatedIdsArray = new UUID[batchSize];
        String[] txIdsArray = new String[batchSize];
        for(int i = 0; i<batchSize; i++) {
            generatedIdsArray[i] = UUID.randomUUID();
            txIdsArray[i] = txIds.get(i);
        }
        jpaTxIdsRepository.batchInsertOrRemoveByHistoryTxIds(generatedIdsArray, txIdsArray);
    }
}