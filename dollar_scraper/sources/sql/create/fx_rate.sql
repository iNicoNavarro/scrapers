--- ******** Creación de la tabla fx_rate **********
-- Los datos provienen de los bancos oficiales por cada país

create table {staging_table}
(
    source_date                 date,
    value                       float8,
    unit                        varchar(5),
    reference_currency          varchar(5),
    source                      TEXT,
    country                     varchar(5)
);


