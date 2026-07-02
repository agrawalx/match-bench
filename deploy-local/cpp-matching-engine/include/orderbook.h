#pragma once
#include "order.h"
#include "trade.h"
#include "types.h"
#include <map>
#include <unordered_map>

class Orderbook {
private:
    // --- Internal bookkeeping ---
    // Pairs an order with its position in the price-level queue.
    // WHY store the iterator? So CancelOrder() can remove the order in O(1)
    // instead of searching through the entire list (which would be O(n)).
    struct OrderEntry {
        OrderPointer order_{ nullptr };
        OrderPointers::iterator location_;
    };

    // Bids sorted DESCENDING (highest price = best bid = first)
    std::map<Price, OrderPointers, std::greater<Price>> bids_;

    // Asks sorted ASCENDING (lowest price = best ask = first)
    std::map<Price, OrderPointers, std::less<Price>> asks_;

    // O(1) lookup of any order by ID.
    // WHY unordered_map? Because cancel/modify need to find orders instantly.
    // std::map would be O(log n) — fine for price levels, but wasteful for ID lookup.
    std::unordered_map<OrderId, OrderEntry> orders_;

    bool CanMatch(Side side, Price price) const;
    Trades MatchOrders();

public:
    // --- Public interface ---
    // These are the only operations the outside world can perform.

    Trades AddOrder(OrderPointer order);
    void   CancelOrder(OrderId orderId);
    Trades ModifyOrder(OrderModify order); 

    std::size_t Size() const { return orders_.size(); }
    OrderbookLevelInfos GetOrderInfos() const;
};
