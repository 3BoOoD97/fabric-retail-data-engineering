CREATE TABLE [dbo].[stg_dim_date] (
    [DateKey]   INT          NULL,
    [FullDate]  DATE         NULL,
    [Day]       INT          NULL,
    [Month]     INT          NULL,
    [MonthName] VARCHAR (20) NULL,
    [Quarter]   INT          NULL,
    [Year]      INT          NULL
);


GO