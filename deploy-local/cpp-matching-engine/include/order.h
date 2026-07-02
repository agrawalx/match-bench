#pragma once
#include "types.h"
// NOTE: original used <format>/std::format, unavailable in the platform build
// image (ubuntu:22.04 -> gcc-11 libstdc++ has no <format>; it lands in gcc-13).
// Swapped to std::to_string concat so the engine compiles in the build-worker.
// Behavior is identical — only the text of two error-path messages changes.
#include <list>
#include <memory>
#include <stdexcept>
#include <string>

class Order {
public:
    Order(OrderType orderType, OrderId orderId, Side side, Price price, Quantity quantity)
        : orderType_{ orderType }
        , orderId_{ orderId }
        , side_{ side }
        , price_{ price }
        , initialQuantity_{ quantity }
        , remainingQuantity_{ quantity }
    { }

    // Delegating constructor for Market orders.
    // WHY? Market orders don't have a meaningful price (they take whatever's available).
    // So we default price to 0 and let AddOrder() convert them to GTC at the worst price.
    Order(OrderId orderId, Side side, Quantity quantity)
        : Order(OrderType::Market, orderId, side, 0, quantity)
    { }

    // All marked 'const' — they promise not to modify the object.
    OrderId   GetOrderId()           const { return orderId_; }
    Side      GetSide()              const { return side_; }
    Price     GetPrice()             const { return price_; }
    OrderType GetOrderType()         const { return orderType_; }
    Quantity  GetInitialQuantity()   const { return initialQuantity_; }
    Quantity  GetRemainingQuantity()  const { return remainingQuantity_; }

    // WHY call GetInitialQuantity() instead of using initialQuantity_ directly?
    // It's a design principle: if you ever add logic to the getter (like clamping
    // or validation), all callers automatically get the new behavior.
    Quantity GetFilledQuantity() const { return GetInitialQuantity() - GetRemainingQuantity(); }
    bool IsFilled() const { return GetRemainingQuantity() == 0; }

    void Fill(Quantity quantity) {
        if (quantity > GetRemainingQuantity())
            throw std::logic_error(
                "Order (" + std::to_string(GetOrderId()) +
                ") cannot be filled for more than its remaining quantity.");

        remainingQuantity_ -= quantity;
    }

    // Converts a Market order to GoodTillCancel at a specific price.
    // WHY does this exist? Market orders need a price to sit in the book's
    // std::map<Price, ...>. We set it to the worst available price so the
    // market order sweeps through all levels.
    void ToGoodTillCancel(Price price) {
        if (GetOrderType() != OrderType::Market)
            throw std::logic_error(
                "Order (" + std::to_string(GetOrderId()) +
                ") cannot have its price adjusted, only market orders can.");

        price_ = price;
        orderType_ = OrderType::GoodTillCancel;
    }

private:
    OrderType orderType_;
    OrderId   orderId_;
    Side      side_;
    Price     price_;
    Quantity  initialQuantity_;
    Quantity  remainingQuantity_;
};

// --- Pointer types ---
// WHY shared_ptr? Multiple things point to the same Order:
// 1. The price-level list (bids_/asks_)
// 2. The orders_ lookup map
// 3. Trade results reference the order
// shared_ptr ensures the Order stays alive as long as anyone references it.

using OrderPointer  = std::shared_ptr<Order>;
using OrderPointers = std::list<OrderPointer>;
// WHY std::list instead of std::vector?
// list gives O(1) insert/erase at any position (via iterator).
// vector would require O(n) shifting when you cancel an order in the middle.
// For an orderbook with thousands of orders per price level, this matters.

// --- OrderModify ---
// Represents a request to modify an existing order.
// In real exchanges, a modify = cancel old + add new (you lose time priority).

class OrderModify {
public:
    OrderModify(OrderId orderId, Side side, Price price, Quantity quantity)
        : orderId_{ orderId }
        , price_{ price }
        , side_{ side }
        , quantity_{ quantity }
    { }

    OrderId  GetOrderId() const { return orderId_; }
    Price    GetPrice()   const { return price_; }
    Side     GetSide()    const { return side_; }
    Quantity GetQuantity() const { return quantity_; }

    // Creates a new Order from the modify request.
    // The OrderType is preserved from the original order (you can't change
    // a GTC order into a FAK by modifying it — that's not how exchanges work).
    OrderPointer ToOrderPointer(OrderType type) const {
        return std::make_shared<Order>(type, GetOrderId(), GetSide(), GetPrice(), GetQuantity());
    }

private:
    OrderId  orderId_;
    Price    price_;
    Side     side_;
    Quantity quantity_;
};
