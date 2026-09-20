CREATE TABLE [dbo].[dim_product] (
    [ProductKey]   INT          NOT NULL,
    [ProductID]    VARCHAR (50) NULL,
    [ProductName]  VARCHAR (80) NULL,
    [CategoryName] VARCHAR (50) NULL
);


GO

ALTER TABLE [dbo].[dim_product]
    ADD CONSTRAINT [PK_dim_product] PRIMARY KEY NONCLUSTERED ([ProductKey] ASC) NOT ENFORCED;


GO