#pragma once
#include <cstdint>
#include <vector>

// --- Enums ---
// WHY enum class instead of plain enum?
// Plain enum leaks names into the global scope: Buy, Sell would clash with
// any other Buy/Sell. enum class forces us to write Side::Buy,
// which is safer.

enum class OrderType {
    GoodTillCancel,  // Stays in book until filled or explicitly cancelled
    FillAndKill,     // Fill whatever you can immediately, cancel the rest
    Market           // Fill at any price — converted to GTC at worst price
};

enum class Side {
    Buy,
    Sell
};

using Price    = std::int32_t;   // Signed — prices can theoretically be negative (oil futures 2020!)
using Quantity = std::uint32_t;  // Unsigned — quantity can't be negative
using OrderId  = std::uint64_t;  // 64-bit — supports billions of unique orders

// --- Level Info ---
// Represents aggregated quantity at a single price level.
// Used for market data snapshots

struct LevelInfo {
    Price price_;
    Quantity quantity_;
};

using LevelInfos = std::vector<LevelInfo>;

// Immutable snapshot of the entire book's visible state.

class OrderbookLevelInfos {
public:
    OrderbookLevelInfos(const LevelInfos& bids, const LevelInfos& asks)
        : bids_{ bids }
        , asks_{ asks }
    { }

    const LevelInfos& GetBids() const { return bids_; }
    const LevelInfos& GetAsks() const { return asks_; }

private:
    LevelInfos bids_;
    LevelInfos asks_;
};
