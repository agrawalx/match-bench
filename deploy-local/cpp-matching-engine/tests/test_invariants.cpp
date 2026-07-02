#include <gtest/gtest.h>
#include "orderbook.h"

#include <random>
#include <vector>
#include <algorithm>


static void AssertOrderbookInvariants(const Orderbook& book) {
    auto infos = book.GetOrderInfos();
    const auto& bids = infos.GetBids();
    const auto& asks = infos.GetAsks();
    for (size_t i = 1; i < bids.size(); ++i) {
        ASSERT_GT(bids[i - 1].price_, bids[i].price_)
            << "Bids not sorted descending at index " << i
            << ": price " << bids[i - 1].price_ << " should be > " << bids[i].price_;
    }
    for (size_t i = 1; i < asks.size(); ++i) {
        ASSERT_LT(asks[i - 1].price_, asks[i].price_)
            << "Asks not sorted ascending at index " << i
            << ": price " << asks[i - 1].price_ << " should be < " << asks[i].price_;
    }
    if (!bids.empty() && !asks.empty()) {
        ASSERT_LT(bids.front().price_, asks.front().price_)
            << "CROSSED BOOK! Best bid " << bids.front().price_
            << " >= best ask " << asks.front().price_
            << ". The matching engine should have matched these.";
    }

    for (const auto& level : bids) {
        ASSERT_GT(level.quantity_, 0u)
            << "Bid level at price " << level.price_ << " has zero quantity — should be removed.";
    }
    for (const auto& level : asks) {
        ASSERT_GT(level.quantity_, 0u)
            << "Ask level at price " << level.price_ << " has zero quantity — should be removed.";
    }
}


TEST(InvariantTest, EmptyBook_InvariantsHold) {
    Orderbook book;
    AssertOrderbookInvariants(book);
}

TEST(InvariantTest, SingleBid_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50));
    AssertOrderbookInvariants(book);
}

TEST(InvariantTest, BidAndAsk_NoMatch_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 99, 50));
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 2, Side::Sell, 101, 50));
    AssertOrderbookInvariants(book); 
}

TEST(InvariantTest, AfterFullMatch_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50));
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 2, Side::Sell, 100, 50));
    AssertOrderbookInvariants(book); 
}

TEST(InvariantTest, AfterPartialMatch_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 100, 100));
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 2, Side::Sell, 100, 30));
    AssertOrderbookInvariants(book); 
}

TEST(InvariantTest, AfterCancel_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50));
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 2, Side::Buy, 99, 50));
    book.CancelOrder(1);
    AssertOrderbookInvariants(book);
}

TEST(InvariantTest, AfterModify_InvariantsHold) {
    Orderbook book;
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50));
    book.AddOrder(std::make_shared<Order>(OrderType::GoodTillCancel, 2, Side::Sell, 105, 50));
    book.ModifyOrder(OrderModify(1, Side::Buy, 103, 50));
    AssertOrderbookInvariants(book);
}


TEST(InvariantTest, RandomOperations_10000_InvariantsAlwaysHold) {
    Orderbook book;
    std::mt19937 rng(42);
    std::uniform_int_distribution<Price> priceDist(90, 110);
    std::uniform_int_distribution<Quantity> qtyDist(1, 1000);

    OrderId nextId = 1;
    std::vector<OrderId> activeIds;

    for (int i = 0; i < 10000; ++i) {
        int action = rng() % 100;

        if (action < 60 || activeIds.empty()) {
            Side side = (rng() % 2) ? Side::Buy : Side::Sell;
            Price price = priceDist(rng);
            Quantity qty = qtyDist(rng);

            auto order = std::make_shared<Order>(
                OrderType::GoodTillCancel, nextId, side, price, qty);
            book.AddOrder(order);
            activeIds.push_back(nextId);
            nextId++;
        }
        else if (action < 85) {
            std::uniform_int_distribution<size_t> idxDist(0, activeIds.size() - 1);
            size_t idx = idxDist(rng);
            book.CancelOrder(activeIds[idx]);
            activeIds.erase(activeIds.begin() + static_cast<long long>(idx));
        }
        else {
            std::uniform_int_distribution<size_t> idxDist(0, activeIds.size() - 1);
            size_t idx = idxDist(rng);
            Side side = (rng() % 2) ? Side::Buy : Side::Sell;
            Price price = priceDist(rng);
            Quantity qty = qtyDist(rng);
            book.ModifyOrder(OrderModify(activeIds[idx], side, price, qty));
        }
        AssertOrderbookInvariants(book);
    }
}


TEST(StressTest, OneMillionOperations_NoCrash) {
    Orderbook book;
    std::mt19937 rng(12345);
    OrderId nextId = 1;
    std::vector<OrderId> activeIds;

    for (int i = 0; i < 1'000'000; ++i) {
        int op = rng() % 100;

        if (op < 60 || activeIds.empty()) {
            Side side = (rng() % 2) ? Side::Buy : Side::Sell;
            OrderType type = static_cast<OrderType>(rng() % 3);
            Price price = static_cast<Price>((rng() % 200) + 1);
            Quantity qty = static_cast<Quantity>((rng() % 10000) + 1);

            if (type == OrderType::Market) {
                auto order = std::make_shared<Order>(nextId, side, qty);
                book.AddOrder(order);
            } else {
                auto order = std::make_shared<Order>(type, nextId, side, price, qty);
                book.AddOrder(order);
            }
            activeIds.push_back(nextId);
            nextId++;
        }
        else if (op < 85) {
            std::uniform_int_distribution<size_t> idxDist(0, activeIds.size() - 1);
            size_t idx = idxDist(rng);
            book.CancelOrder(activeIds[idx]);
            activeIds.erase(activeIds.begin() + static_cast<long long>(idx));
        }
        else {
            std::uniform_int_distribution<size_t> idxDist(0, activeIds.size() - 1);
            size_t idx = idxDist(rng);
            Side side = (rng() % 2) ? Side::Buy : Side::Sell;
            Price price = static_cast<Price>((rng() % 200) + 1);
            Quantity qty = static_cast<Quantity>((rng() % 10000) + 1);
            book.ModifyOrder(OrderModify(activeIds[idx], side, price, qty));
        }
    }

    AssertOrderbookInvariants(book);
    SUCCEED();
}
