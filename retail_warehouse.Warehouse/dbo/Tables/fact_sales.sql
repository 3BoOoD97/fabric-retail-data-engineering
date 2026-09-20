CREATE TABLE [dbo].[fact_sales] (
    [OrderID]        VARCHAR (50)    NOT NULL,
    [CustomerKey]    INT             NOT NULL,
    [ProductKey]     INT             NOT NULL,
    [DateKey]        INT             NOT NULL,
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

ALTER TABLE [dbo].[fact_sales]
    ADD CONSTRAINT [FK_fact_customer] FOREIGN KEY ([CustomerKey]) REFERENCES [dbo].[dim_customer] ([CustomerKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[fact_sales]
    ADD CONSTRAINT [FK_fact_date] FOREIGN KEY ([DateKey]) REFERENCES [dbo].[dim_date] ([DateKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[fact_sales]
    ADD CONSTRAINT [FK_fact_product] FOREIGN KEY ([ProductKey]) REFERENCES [dbo].[dim_product] ([ProductKey]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[fact_sales]
    ADD CONSTRAINT [PK_fact_sales] PRIMARY KEY NONCLUSTERED ([OrderID] ASC, [ProductKey] ASC) NOT ENFORCED;


GO