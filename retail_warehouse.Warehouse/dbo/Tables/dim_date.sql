CREATE TABLE [dbo].[dim_date] (
    [DateKey]   INT          NOT NULL,
    [FullDate]  DATE         NULL,
    [Day]       INT          NULL,
    [Month]     INT          NULL,
    [MonthName] VARCHAR (20) NULL,
    [Quarter]   INT          NULL,
    [Year]      INT          NULL
);


GO

ALTER TABLE [dbo].[dim_date]
    ADD CONSTRAINT [PK_dim_date] PRIMARY KEY NONCLUSTERED ([DateKey] ASC) NOT ENFORCED;


GO