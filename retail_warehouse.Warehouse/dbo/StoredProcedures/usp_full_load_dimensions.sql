CREATE   PROCEDURE dbo.usp_full_load_dimensions
AS
BEGIN

    INSERT INTO dbo.dim_customer
    (
        CustomerID,
        Gender,
        Region,
        City,
        Age,
        CustomerSegment,
        SignUpDate
    )
    SELECT
        CustomerID,
        Gender,
        Region,
        City,
        Age,
        CustomerSegment,
        SignUpDate
    FROM dbo.stg_dim_customer;


    INSERT INTO dbo.dim_product
    (
        ProductID,
        ProductName,
        CategoryName
    )
    SELECT
        ProductID,
        ProductName,
        CategoryName
    FROM dbo.stg_dim_product;

END;

GO