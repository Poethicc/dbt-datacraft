select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




SELECT 'Unknown key' WHERE 1!=1

      
    ) dbt_internal_test