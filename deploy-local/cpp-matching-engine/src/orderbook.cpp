#include "orderbook.h"

#include <numeric>

// --- CanMatch ---
// Checks if an incoming order at the given price would cross the spread.
// "Crossing the spread" means a buyer is willing to pay >= the cheapest seller's price,
// or a seller is willing to accept <= the richest buyer's price.

bool Orderbook::CanMatch(Side side, Price price) const {
    if (side == Side::Buy) {
        if (asks_.empty())
            return false;
        const auto& [bestAsk, _] = *asks_.begin();
        return price >= bestAsk;
    }
    else {
        if (bids_.empty())
            return false;
        const auto& [bestBid, _] = *bids_.begin();
        return price <= bestBid;
    }
}

// --- MatchOrders ---
// The core matching engine. Called after every AddOrder().
// Implements Price-Time Priority (the standard matching algorithm):
//   1. Best price gets filled first
//   2. At the same price, whoever arrived first gets filled first (FIFO)

Trades Orderbook::MatchOrders() {
    Trades trades;
    trades.reserve(orders_.size());

    while (true) {
        if (bids_.empty() || asks_.empty())
            break;

        auto& [bidPrice, bids] = *bids_.begin();
        auto& [askPrice, asks] = *asks_.begin();

        // If the best bid is below the best ask, no trade is possible.
        // The "spread" (askPrice - bidPrice) is positive — no one agrees on a price.
        if (bidPrice < askPrice)
            break;

        // Match orders at the top of each queue
        while (bids.size() && asks.size()) {
            auto bid = bids.front();
            auto ask = asks.front();

            // Trade quantity = minimum of what each side wants.
            Quantity quantity = std::min(bid->GetRemainingQuantity(), ask->GetRemainingQuantity());

            bid->Fill(quantity);
            ask->Fill(quantity);

            // Remove fully-filled orders from the book
            if (bid->IsFilled()) {
                bids.pop_front();
                orders_.erase(bid->GetOrderId());
            }
            if (ask->IsFilled()) {
                asks.pop_front();
                orders_.erase(ask->GetOrderId());
            }

            // Clean up empty price levels
            if (bids.empty())
                bids_.erase(bidPrice);

            if (asks.empty())
                asks_.erase(askPrice);

            // Record the trade
            trades.push_back(Trade{
                TradeInfo{ bid->GetOrderId(), bid->GetPrice(), quantity },
                TradeInfo{ ask->GetOrderId(), ask->GetPrice(), quantity }
            });
        }
    }

    // --- Fill-and-Kill cleanup ---
    // If a FAK order wasn't fully filled, cancel whatever remains.
    // WHY only check the front? Because FAK orders are always the aggressor
    // (the new incoming order), and they sit at the front of their queue.
    if (!bids_.empty()) {
        auto& [_, bids] = *bids_.begin();
        auto& order = bids.front();
        if (order->GetOrderType() == OrderType::FillAndKill)
            CancelOrder(order->GetOrderId());
    }
    if (!asks_.empty()) {
        auto& [_, asks] = *asks_.begin();
        auto& order = asks.front();
        if (order->GetOrderType() == OrderType::FillAndKill)
            CancelOrder(order->GetOrderId());
    }

    return trades;
}

// --- AddOrder ---
// The main entry point for new orders.
// Returns any trades that resulted from adding this order.

Trades Orderbook::AddOrder(OrderPointer order) {
    // Reject duplicate order IDs.
    if (orders_.contains(order->GetOrderId()))
        return { };

    // --- Market order handling ---
    if (order->GetOrderType() == OrderType::Market) {
        if (order->GetSide() == Side::Buy && !asks_.empty()) {
            // Convert to GTC at the WORST (highest) ask price.
            // WHY worst? Because a market buy says "I want to buy
            // no matter what the price is." Setting price = worst ask ensures
            // the order can match against ALL ask levels, not just the best.
            const auto& [worstAsk, _] = *asks_.rbegin();
            order->ToGoodTillCancel(worstAsk);
        }
        else if (order->GetSide() == Side::Sell && !bids_.empty()) {
            const auto& [worstBid, _] = *bids_.rbegin();
            order->ToGoodTillCancel(worstBid);
        }
        else {
            return { };
        }
    }

    // --- Fill-and-Kill pre-check ---
    // If this FAK order can't match anything, reject it immediately.
    if (order->GetOrderType() == OrderType::FillAndKill && !CanMatch(order->GetSide(), order->GetPrice()))
        return { };

    // --- Insert into the book ---
    OrderPointers::iterator iterator;
    if (order->GetSide() == Side::Buy) {
        auto& orders = bids_[order->GetPrice()];
        orders.push_back(order);
        // std::prev(orders.end()) points to the LAST element.
        // We store this iterator so CancelOrder can remove it in O(1).
        iterator = std::prev(orders.end());
    }
    else {
        auto& orders = asks_[order->GetPrice()];
        orders.push_back(order);
        iterator = std::prev(orders.end());
    }

    orders_.insert({ order->GetOrderId(), OrderEntry{ order, iterator } });

    // Try to match after insertion
    return MatchOrders();
}

// --- CancelOrder ---
// Removes an order from the book.

void Orderbook::CancelOrder(OrderId orderId) {
    if (!orders_.contains(orderId))
        return;

    const auto& entry = orders_.at(orderId);
    auto order = entry.order_;
    auto orderIterator = entry.location_;

    if (order->GetSide() == Side::Sell) {
        auto price = order->GetPrice();
        auto& orders = asks_.at(price);
        orders.erase(orderIterator);
        if (orders.empty())
            asks_.erase(price);
    }
    else {
        auto price = order->GetPrice();
        auto& orders = bids_.at(price);
        orders.erase(orderIterator);
        if (orders.empty())
            bids_.erase(price);
    }

    orders_.erase(orderId);
}

// --- ModifyOrder ---
// Modifies an existing order. Implemented as cancel + re-add.

Trades Orderbook::ModifyOrder(OrderModify order) {
    if (!orders_.contains(order.GetOrderId()))
        return { };

    auto existingOrder = orders_.at(order.GetOrderId()).order_;
    CancelOrder(order.GetOrderId());
    return AddOrder(order.ToOrderPointer(existingOrder->GetOrderType()));
}

// --- GetOrderInfos ---
// Creates an immutable snapshot of the current book state.

OrderbookLevelInfos Orderbook::GetOrderInfos() const {
    LevelInfos bidInfos, askInfos;
    bidInfos.reserve(orders_.size());
    askInfos.reserve(orders_.size());

    // Lambda that aggregates total quantity at a price level.
    // std::accumulate walks through all orders at this price and sums their
    // remaining quantities. This gives the total visible size at each level.
    auto CreateLevelInfos = [](Price price, const OrderPointers& orders) {
        return LevelInfo{
            price,
            std::accumulate(orders.begin(), orders.end(), (Quantity)0,
                [](Quantity runningSum, const OrderPointer& order) {
                    return runningSum + order->GetRemainingQuantity();
                })
        };
    };

    for (const auto& [price, orders] : bids_)
        bidInfos.push_back(CreateLevelInfos(price, orders));

    for (const auto& [price, orders] : asks_)
        askInfos.push_back(CreateLevelInfos(price, orders));

    return OrderbookLevelInfos{ bidInfos, askInfos };
}
