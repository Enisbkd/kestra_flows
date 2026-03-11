CREATE TABLE default.fintrans (
      financialTransactionDetails ROW<
      trxNo BIGINT,
      folioNo INT,
      postingDate TIMESTAMP,
      pricePerUnit DOUBLE,
      roomClass VARCHAR,
        ...>,
       reservationDetails ROW<resort VARCHAR, resvNameid BIGINT>,
                                  roomDetails ROW<room VARCHAR>,
                                  folioTaxDetails ROW<deposit DOUBLE, taxAmount9 DOUBLE>,
                                  updateDate TIMESTAMP
)
    PARTITIONED BY (days(updateDate))
WITH (
  format = 'parquet'
);