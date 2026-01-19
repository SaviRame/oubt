
-- vendor
select * from bronze.vendor order by vendor_id;
select * from silver.vendor order by vendor_id;
select * from gold.dim_vendor;
select * from gold.xref_vendor_scd2 order by vendor_id, valid_from;








