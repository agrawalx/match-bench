#include <gtest/gtest.h>
#include "order.h"

TEST(OrderConstructionTest, GoodTillCancel_AllFieldsSet) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    EXPECT_EQ(order.GetOrderType(), OrderType::GoodTillCancel);
    EXPECT_EQ(order.GetOrderId(), 1);
    EXPECT_EQ(order.GetSide(), Side::Buy);
    EXPECT_EQ(order.GetPrice(), 100);
    EXPECT_EQ(order.GetInitialQuantity(), 50);
    EXPECT_EQ(order.GetRemainingQuantity(), 50);
    EXPECT_EQ(order.GetFilledQuantity(), 0); 
    EXPECT_FALSE(order.IsFilled());
}

TEST(OrderConstructionTest, MarketOrder_PriceIsZero) {
    Order order(42, Side::Sell, 200);
    
    EXPECT_EQ(order.GetOrderType(), OrderType::Market);
    EXPECT_EQ(order.GetPrice(), 0);
    EXPECT_EQ(order.GetInitialQuantity(), 200);
}


TEST(OrderFillTest, PartialFill_ReducesRemainingQuantity) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    
    order.Fill(20);
    
    EXPECT_EQ(order.GetRemainingQuantity(), 30);   
    EXPECT_EQ(order.GetFilledQuantity(), 20);       
    EXPECT_FALSE(order.IsFilled());                  
}

TEST(OrderFillTest, MultipleFills_AccumulateCorrectly) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 100);
    
    order.Fill(30);
    EXPECT_EQ(order.GetRemainingQuantity(), 70);
    
    order.Fill(30);
    EXPECT_EQ(order.GetRemainingQuantity(), 40);
    
    order.Fill(40);
    EXPECT_EQ(order.GetRemainingQuantity(), 0);
    EXPECT_TRUE(order.IsFilled());
}

TEST(OrderFillTest, ExactFill_OrderBecomesFullyFilled) {
    Order order(OrderType::GoodTillCancel, 1, Side::Sell, 50, 75);
    
    order.Fill(75);
    
    EXPECT_EQ(order.GetRemainingQuantity(), 0);
    EXPECT_EQ(order.GetFilledQuantity(), 75);
    EXPECT_TRUE(order.IsFilled());
}

TEST(OrderFillTest, OverFill_ThrowsException) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    
    EXPECT_THROW(order.Fill(51), std::logic_error);
    EXPECT_EQ(order.GetRemainingQuantity(), 50);
}

TEST(OrderFillTest, ZeroFill_IsNoOp) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    
    order.Fill(0);
    
    EXPECT_EQ(order.GetRemainingQuantity(), 50);
    EXPECT_FALSE(order.IsFilled());
}

TEST(OrderFillTest, FillAlreadyFilledOrder_Throws) {
    Order order(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    order.Fill(50); 
    
    EXPECT_TRUE(order.IsFilled());
    EXPECT_THROW(order.Fill(1), std::logic_error); 
}

TEST(OrderConversionTest, MarketToGTC_SetsCorrectPriceAndType) {
    Order order(1, Side::Buy, 100);
    
    EXPECT_EQ(order.GetOrderType(), OrderType::Market);
    EXPECT_EQ(order.GetPrice(), 0);
    
    order.ToGoodTillCancel(95); 
    
    EXPECT_EQ(order.GetOrderType(), OrderType::GoodTillCancel);
    EXPECT_EQ(order.GetPrice(), 95);
}

TEST(OrderConversionTest, NonMarketToGTC_Throws) {
    Order gtcOrder(OrderType::GoodTillCancel, 1, Side::Buy, 100, 50);
    EXPECT_THROW(gtcOrder.ToGoodTillCancel(95), std::logic_error);
    
    Order fakOrder(OrderType::FillAndKill, 2, Side::Sell, 100, 50);
    EXPECT_THROW(fakOrder.ToGoodTillCancel(95), std::logic_error);
}

TEST(OrderConversionTest, MarketToGTC_CannotConvertTwice) {
    Order order(1, Side::Buy, 100);
    order.ToGoodTillCancel(95);
    EXPECT_THROW(order.ToGoodTillCancel(90), std::logic_error);
}


TEST(OrderModifyTest, ToOrderPointer_CreatesCorrectOrder) {
    OrderModify modify(42, Side::Buy, 105, 200);
    
    OrderPointer newOrder = modify.ToOrderPointer(OrderType::GoodTillCancel);
    
    EXPECT_EQ(newOrder->GetOrderId(), 42);
    EXPECT_EQ(newOrder->GetSide(), Side::Buy);
    EXPECT_EQ(newOrder->GetPrice(), 105);
    EXPECT_EQ(newOrder->GetInitialQuantity(), 200);
    EXPECT_EQ(newOrder->GetOrderType(), OrderType::GoodTillCancel);
}

TEST(OrderModifyTest, PreservesOrderType) {
    OrderModify modify(1, Side::Sell, 100, 50);
    
    auto gtcOrder = modify.ToOrderPointer(OrderType::GoodTillCancel);
    auto fakOrder = modify.ToOrderPointer(OrderType::FillAndKill);
    
    EXPECT_EQ(gtcOrder->GetOrderType(), OrderType::GoodTillCancel);
    EXPECT_EQ(fakOrder->GetOrderType(), OrderType::FillAndKill);
}
