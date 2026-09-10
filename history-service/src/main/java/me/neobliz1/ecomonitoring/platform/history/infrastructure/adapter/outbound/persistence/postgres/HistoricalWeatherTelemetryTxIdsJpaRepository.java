package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherTelemetryStationTransaction;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface HistoricalWeatherTelemetryTxIdsJpaRepository extends JpaRepository<WeatherTelemetryStationTransaction, UUID> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            WITH incoming AS (
                 SELECT * FROM UNNEST(CAST(:ids AS uuid[]), CAST(:txIdsHistory AS varchar[])) AS t(id, tx_id_history)
            ),
            deleted AS (
                DELETE FROM #{#entityName} t
                USING incoming i
                WHERE t.tx_id_ingestion = i.tx_id_history
                RETURNING t.tx_id_ingestion
            )
            INSERT INTO #{#entityName} (id, tx_id_ingestion, tx_id_history)
            SELECT i.id, NULL, i.tx_id_history
            FROM incoming i
            WHERE NOT EXISTS (
                SELECT 1 FROM deleted d
                WHERE d.tx_id_ingestion = i.tx_id_history
            )
            AND NOT EXISTS (
                SELECT 1 FROM #{#entityName} t
                WHERE t.tx_id_history = i.tx_id_history
            )
            """, nativeQuery = true)
    void batchInsertOrRemoveByHistoryTxIds(@Param("ids") UUID[] ids,
                                           @Param("txIdsHistory") String[] txIdsHistory);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            WITH deleted AS (
                DELETE FROM #{#entityName}
                WHERE tx_id_history = :txIdIngestion
                RETURNING 1
            )
            INSERT INTO #{#entityName} (id, tx_id_ingestion, tx_id_history)
            SELECT :id, :txIdIngestion, NULL
            WHERE NOT EXISTS (SELECT 1 FROM deleted)
              AND NOT EXISTS (
                  SELECT 1 FROM #{#entityName}
                  WHERE tx_id_ingestion = :txIdIngestion
              )
            """, nativeQuery = true)
    void insertOrRemoveByIngestionTxId(@Param("id") UUID id, @Param("txIdIngestion") String txIdIngestion);
}
