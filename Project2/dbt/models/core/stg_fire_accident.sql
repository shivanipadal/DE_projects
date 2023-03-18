{{ config(materialized='table') }}

select 
  incidentnumber,
  exposurenumber,
  id,
  address,
  cast(incidentdate as DATETIME) as incidentdate,
  extract(year from cast(incidentdate as DATETIME)) as incidentyear,
  extract(month from cast(incidentdate as DATETIME)) as incidentmonth,
  callnumber,
  cast(alarmdttm as DATETIME) as alarmdttm,
  cast(arrivaldttm as DATETIME) as arrivaldttm,
  TIMESTAMP_DIFF(cast(arrivaldttm as TIMESTAMP), cast(alarmdttm as TIMESTAMP),MINUTE) as delayarrivalsec,
  cast(closedttm as DATETIME) as closedttm,
  city,
  zipcode,
  battalion,
  stationarea,
  numberofalarms,
  actiontakenprimary,
  actiontakensecondary,
  actiontakenother,
  areaoffireorigin

  from 
    {{ source('staging', 'fire_data_sanfrancisco')}}




