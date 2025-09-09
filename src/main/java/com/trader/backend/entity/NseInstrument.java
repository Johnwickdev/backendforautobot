package com.trader.backend.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@Document(collection = "nse_instruments")
public class NseInstrument {

    @Id
    @JsonProperty("instrument_key")
    private String instrumentKey;

    @JsonProperty("weekly")
    @Field("weekly")
    private boolean weekly;

    @JsonProperty("segment")
    @Field("segment")
    private String segment;

    @JsonProperty("name")
    @Field("name")
    private String name;

    @JsonProperty("exchange")
    @Field("exchange")
    private String exchange;

    @JsonProperty("expiry")
    @Field("expiry")
    private long expiry;

    @JsonProperty("instrument_type")
    @Field("instrument_type")
    private String instrumentType;

    @JsonProperty("strike_price")
    @Field("strike_price")
    private Integer strikePrice;

    @JsonProperty("asset_symbol")
    @Field("asset_symbol")
    private String assetSymbol;

    @JsonProperty("underlying_symbol")
    @Field("underlying_symbol")
    private String underlyingSymbol;

    @JsonProperty("lot_size")
    @Field("lot_size")
    private int lotSize;

    @JsonProperty("freeze_quantity")
    @Field("freeze_quantity")
    private double freezeQuantity;

    @JsonProperty("exchange_token")
    @Field("exchange_token")
    private String exchangeToken;

    @JsonProperty("minimum_lot")
    @Field("minimum_lot")
    private int minimumLot;

    @JsonProperty("asset_key")
    @Field("asset_key")
    private String assetKey;

    @JsonProperty("underlying_key")
    @Field("underlying_key")
    private String underlyingKey;

    @JsonProperty("tick_size")
    @Field("tick_size")
    private double tickSize;

    @JsonProperty("asset_type")
    @Field("asset_type")
    private String assetType;

    @JsonProperty("underlying_type")
    @Field("underlying_type")
    private String underlyingType;

    @JsonProperty("trading_symbol")
    @Field("trading_symbol")
    private String tradingSymbol;

    @JsonProperty("qty_multiplier")
    @Field("qty_multiplier")
    private double qtyMultiplier;
}
