CREATE TABLE [dbo].[stg_fact_sales] (
    [OrderID]        VARCHAR (50)    NULL,
    [CustomerID]     VARCHAR (50)    NULL,
    [ProductID]      VARCHAR (50)    NULL,
    [OrderDate]      DATE            NULL,
    [Quantity]       INT             NULL,
    [UnitCost]       DECIMAL (18, 2) NULL,
    [UnitPrice]      DECIMAL (18, 2) NULL,
    [DiscountRate]   DECIMAL (5, 4)  NULL,
    [GrossAmount]    DECIMAL (18, 2) NULL,
    [DiscountAmount] DECIMAL (18, 2) NULL,
    [NetAmount]      DECIMAL (18, 2) NULL,
    [CostAmount]     DECIMAL (18, 2) NULL,
    [ProfitAmount]   DECIMAL (18, 2) NULL,
    [ReturnAmount]   DECIMAL (18, 2) NULL
);


GO