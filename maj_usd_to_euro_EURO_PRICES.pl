#!/usr/bin/perl
# Ce programme a été créé pour mettre à jour les tarifs qui sont négociés en Euros 
# Il doit etre lancé lorsque l'on change le taux EUR USD Freight

use strict;

use DBI;
use Class::Date qw(:errors date now);
use File::Copy;
use HTML::Template;

use lib "/home/fcs/cot_fret/yrocher/web/params/";    # permet d'ajouter des repertoires à @INC
use lib "/home/fcs/cot_fret/yrocher/web/lib/";       # permet d'ajouter des repertoires à @INC
use cot_yr_conf;
use cot_yr_procetfonc;

# Tentative de connexion à la base:
my $dbh = DBI->connect( $base_fcs_dsn, $base_fcs_user, $base_fcs_password, { AutoCommit => 1 } );


###########################################################
my $effective_date = '20161017';
my $old_rate = '1.1155';
my $new_rate = '1.11';
###########################################################


my $str_rate = $new_rate.'/'.$old_rate;

my $purge = 1;
my $dbug = 0;


my $sqlr_del_trips_costs = "
DELETE FROM ref_trips_costs WHERE effective_date = '$effective_date';
";
my $rs_del_trips_costs = $dbh->prepare( $sqlr_del_trips_costs );
print $sqlr_del_trips_costs if($dbug);
$rs_del_trips_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_trips_costs;
  $rs_del_trips_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_trips_costs->finish;


my $sqlr_del_trips_costs_air = "
DELETE FROM ref_trips_costs_air WHERE id_trip_cost NOT IN ( SELECT id FROM ref_trips_costs );
";

my $rs_del_trips_costs_air = $dbh->prepare( $sqlr_del_trips_costs_air );
print $sqlr_del_trips_costs_air if($dbug);
$rs_del_trips_costs_air->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_trips_costs_air;
  $rs_del_trips_costs_air->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_trips_costs_air->finish;

my $sqlr_del_trips_costs_fcl = "
DELETE FROM ref_trips_costs_fcl WHERE id_trip_cost NOT IN ( SELECT id FROM ref_trips_costs );
";
my $rs_del_trips_costs_fcl = $dbh->prepare( $sqlr_del_trips_costs_fcl );
print $sqlr_del_trips_costs_fcl if($dbug);
$rs_del_trips_costs_fcl->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_trips_costs_fcl;
  $rs_del_trips_costs_fcl->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_trips_costs_fcl->finish;

my $sqlr_del_truckings_cost_by_date = "
DELETE FROM ref_truckings_cost_by_date WHERE effective_date = '$effective_date';
";
my $rs_del_truckings_cost_by_date = $dbh->prepare( $sqlr_del_truckings_cost_by_date );
print $sqlr_del_truckings_cost_by_date if($dbug);
$rs_del_truckings_cost_by_date->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_truckings_cost_by_date;
  $rs_del_truckings_cost_by_date->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_truckings_cost_by_date->finish;

my $sqlr_del_palletizations_costs = "
DELETE FROM ref_palletizations_costs WHERE effective_date = '$effective_date';
";
my $rs_del_palletizations_costs = $dbh->prepare( $sqlr_del_palletizations_costs );
print $sqlr_del_palletizations_costs if($dbug);
$rs_del_palletizations_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_palletizations_costs;
  $rs_del_palletizations_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_palletizations_costs->finish;

my $sqlr_del_palletizations_costs_by_pallet = "
DELETE FROM ref_palletizations_costs_by_pallet WHERE id_palletization_cost NOT IN ( SELECT id FROM ref_palletizations_costs );
";
my $rs_del_palletizations_costs_by_pallet = $dbh->prepare( $sqlr_del_palletizations_costs_by_pallet );
print $sqlr_del_palletizations_costs_by_pallet if($dbug);
$rs_del_palletizations_costs_by_pallet->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_palletizations_costs_by_pallet;
  $rs_del_palletizations_costs_by_pallet->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_palletizations_costs_by_pallet->finish;

my $sqlr_del_final_truckings_costs = "
DELETE FROM ref_final_truckings_costs WHERE effective_date = '$effective_date';
";
my $rs_del_final_truckings_costs = $dbh->prepare( $sqlr_del_final_truckings_costs );
print $sqlr_del_final_truckings_costs if($dbug);
$rs_del_final_truckings_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_final_truckings_costs;
  $rs_del_final_truckings_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_final_truckings_costs->finish;

my $sqlr_del_final_truckings_cost_by_nb_pallets = "
DELETE FROM ref_final_truckings_cost_by_nb_pallets WHERE id_final_trucking_cost NOT IN ( SELECT id FROM ref_final_truckings_costs );
";
my $rs_del_final_truckings_cost_by_nb_pallets = $dbh->prepare( $sqlr_del_final_truckings_cost_by_nb_pallets );
print $sqlr_del_final_truckings_cost_by_nb_pallets if($dbug);
$rs_del_final_truckings_cost_by_nb_pallets->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_final_truckings_cost_by_nb_pallets;
  $rs_del_final_truckings_cost_by_nb_pallets->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_final_truckings_cost_by_nb_pallets->finish;

my $sqlr_del_tarifs_ports_costs = "
DELETE FROM ref_tarifs_ports_costs WHERE effective_date = '$effective_date';
";
my $rs_del_tarifs_ports_costs = $dbh->prepare( $sqlr_del_tarifs_ports_costs );
print $sqlr_del_tarifs_ports_costs if($dbug);
$rs_del_tarifs_ports_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_tarifs_ports_costs;
  $rs_del_tarifs_ports_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_tarifs_ports_costs->finish;

my $sqlr_del_ports_costs_fcl_port_tax = "
DELETE FROM ref_tarifs_ports_costs_fcl_port_tax WHERE id_tarif_port_cost NOT IN ( SELECT id FROM ref_tarifs_ports_costs );
";
my $rs_del_ports_costs_fcl_port_tax = $dbh->prepare( $sqlr_del_ports_costs_fcl_port_tax );
print $sqlr_del_ports_costs_fcl_port_tax if($dbug);
$rs_del_ports_costs_fcl_port_tax->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_ports_costs_fcl_port_tax;
  $rs_del_ports_costs_fcl_port_tax->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_ports_costs_fcl_port_tax->finish;

my $sqlr_del_costs_fcl_vendor_management = "
DELETE FROM ref_tarifs_ports_costs_fcl_vendor_management WHERE id_tarif_port_cost NOT IN ( SELECT id FROM ref_tarifs_ports_costs );
";
my $rs_del_costs_fcl_vendor_management = $dbh->prepare( $sqlr_del_costs_fcl_vendor_management );
print $sqlr_del_costs_fcl_vendor_management if($dbug);
$rs_del_costs_fcl_vendor_management->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_costs_fcl_vendor_management;
  $rs_del_costs_fcl_vendor_management->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_costs_fcl_vendor_management->finish;

my $sqlr_del_costs_thc_air = "
DELETE FROM ref_tarifs_ports_costs_thc_air WHERE id_tarif_port_cost NOT IN ( SELECT id FROM ref_tarifs_ports_costs );
";
my $rs_del_costs_thc_air = $dbh->prepare( $sqlr_del_costs_thc_air );
print $sqlr_del_costs_thc_air if($dbug);
$rs_del_costs_thc_air->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_del_costs_thc_air;
  $rs_del_costs_thc_air->finish;
  $dbh->disconnect;
  exit;
}
$rs_del_costs_thc_air->finish;


if ($purge){

	my $sqlr_del_global_costs = "
	DELETE FROM ref_global_costs WHERE effective_date = '$effective_date';";

	my $rs_del_global_costs = $dbh->prepare( $sqlr_del_global_costs );
	print $sqlr_del_global_costs if($dbug);
	$rs_del_global_costs->execute() if(!$dbug);
	if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
	  print "\nError : ".$dbh->errstr."\n". $sqlr_del_global_costs;
	  $rs_del_global_costs->finish;
	  $dbh->disconnect;
	  exit;
	}
	$rs_del_global_costs->finish;
}


my $sqlr_insert_tarifs_ports_costs = " 

----------------
-- MAJ TARIFS PORTS
----------------

INSERT INTO ref_tarifs_ports_costs (
id_ref_tarifs_port, 
effective_date, 
loading_cost_fcl, 
loading_cost_lcl, 
unloading_cost_fcl, 
unloading_cost_lcl, 
bl_cost, 
pss_cost_fcl,  
pss_cost_lcl, 
vendor_management_lcl, 
general_receiver_tax, 
custom_payment_delay, 
port_tax_lcl, 
contract_nb_nomenclature, 
contract_nb_nomenclature_cost, 
out_of_contract_nb_nomenclature, 
out_of_contract_nb_nomenclature_cost, 
licence, 
douane, 
air_thc_min, 
irc, 
unloading_cost_lcl_refer, 
unloading_cost_fcl_refer
)

SELECT 
id_ref_tarifs_port, 
'$effective_date' as effective_date, 
loading_cost_fcl, 
loading_cost_lcl, 
CASE
	WHEN TRIM(CAST(rtpc.unloading_cost_fcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.unloading_cost_fcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as unloading_cost_fcl, 
CASE
	WHEN TRIM(CAST(rtpc.unloading_cost_lcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.unloading_cost_lcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as unloading_cost_lcl, 
CASE
	WHEN TRIM(CAST(rtpc.bl_cost AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.bl_cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as bl_cost, 
CASE
	WHEN TRIM(CAST(rtpc.pss_cost_fcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.pss_cost_fcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as pss_cost_fcl, 
CASE
	WHEN TRIM(CAST(rtpc.pss_cost_lcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.pss_cost_lcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as pss_cost_lcl, 
CASE
	WHEN TRIM(CAST(rtpc.vendor_management_lcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.vendor_management_lcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as vendor_management_lcl, 
rtpc.general_receiver_tax, 
custom_payment_delay, 
CASE
	WHEN TRIM(CAST(rtpc.port_tax_lcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.port_tax_lcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as port_tax_lcl, 
contract_nb_nomenclature, 
CASE
	WHEN TRIM(CAST(rtpc.contract_nb_nomenclature_cost AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.contract_nb_nomenclature_cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as contract_nb_nomenclature_cost, 
out_of_contract_nb_nomenclature, 
CASE
	WHEN TRIM(CAST(rtpc.out_of_contract_nb_nomenclature_cost AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.out_of_contract_nb_nomenclature_cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as out_of_contract_nb_nomenclature_cost, 
CASE
	WHEN TRIM(CAST(rtpc.licence AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.licence as NUMERIC)*($str_rate), 2)
	ELSE 0
END as licence, 
CASE
	WHEN TRIM(CAST(rtpc.douane AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.douane as NUMERIC)*($str_rate), 2)
	ELSE 0
END as douane, 
CASE
	WHEN TRIM(CAST(rtpc.air_thc_min AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.air_thc_min as NUMERIC)*($str_rate), 2)
	ELSE 0
END as air_thc_min, 
CASE
	WHEN TRIM(CAST(rtpc.irc AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.irc as NUMERIC)*($str_rate), 2)
	ELSE 0
END as irc, 
CASE
	WHEN TRIM(CAST(rtpc.unloading_cost_lcl_refer AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.unloading_cost_lcl_refer as NUMERIC)*($str_rate), 2)
	ELSE 0
END as unloading_cost_lcl_refer, 
CASE
	WHEN TRIM(CAST(rtpc.unloading_cost_fcl_refer AS VARCHAR))<>'' THEN ROUND(CAST(rtpc.unloading_cost_fcl_refer as NUMERIC)*($str_rate), 2)
	ELSE 0
END as unloading_cost_fcl_refer

FROM ref_tarifs_ports as rtp
LEFT JOIN ref_tarifs_ports_costs as rtpc
ON rtp.id = rtpc.id_ref_tarifs_port

WHERE CAST(rtp.id as VARCHAR)||CAST(rtp.id_transport_mode AS VARCHAR)||CAST(rtpc.effective_date AS VARCHAR)
IN (
	SELECT
	CAST(rtp.id AS VARCHAR)||CAST(rtp.id_transport_mode AS VARCHAR)||CAST(MAX(rtpc.effective_date) AS VARCHAR)
	FROM ref_tarifs_ports as rtp
	LEFT JOIN ref_tarifs_ports_costs as rtpc
	ON rtp.id = rtpc.id_ref_tarifs_port
	WHERE rtpc.effective_date < '$effective_date'
	GROUP BY rtp.id, rtp.id_transport_mode
);

";

my $rs_insert_tarifs_ports_costs = $dbh->prepare( $sqlr_insert_tarifs_ports_costs );
print $sqlr_insert_tarifs_ports_costs if($dbug);
$rs_insert_tarifs_ports_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_tarifs_ports_costs;
  $rs_insert_tarifs_ports_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_tarifs_ports_costs->finish;

my $sqlr_insert_ports_costs_fcl_port_tax = "

INSERT INTO ref_tarifs_ports_costs_fcl_port_tax ( id_tarif_port_cost, id_ct_kind, port_tax_fcl )
SELECT 
mrtpc.id as id_tarif_port_cost, 
id_ct_kind,
CASE
	WHEN TRIM(CAST(rtpcf.port_tax_fcl AS VARCHAR))<>'' THEN ROUND(CAST(rtpcf.port_tax_fcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as port_tax_fcl 

FROM ref_tarifs_ports_costs_fcl_port_tax as rtpcf
LEFT JOIN ref_tarifs_ports_costs as rtpc
ON rtpcf.id_tarif_port_cost = rtpc.id
LEFT JOIN 
(
  SELECT MAX(id) as id, id_ref_tarifs_port
  FROM ref_tarifs_ports_costs
  GROUP BY id_ref_tarifs_port
)
as mrtpc
ON mrtpc.id_ref_tarifs_port = rtpc.id_ref_tarifs_port

WHERE rtpc.id IN (
  SELECT MAX(id)
  FROM ref_tarifs_ports_costs as rtpc
  WHERE rtpc.effective_date <> '$effective_date'
  AND rtpc.effective_date < '$effective_date'
  GROUP BY id_ref_tarifs_port
);
";
my $rs_insert_ports_costs_fcl_port_tax = $dbh->prepare( $sqlr_insert_ports_costs_fcl_port_tax );
print $sqlr_insert_ports_costs_fcl_port_tax if($dbug);
$rs_insert_ports_costs_fcl_port_tax->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_ports_costs_fcl_port_tax;
  $rs_insert_ports_costs_fcl_port_tax->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_ports_costs_fcl_port_tax->finish;

my $sqlr_insert_fcl_vendor_management = "
INSERT INTO ref_tarifs_ports_costs_fcl_vendor_management ( id_tarif_port_cost, id_ct_kind, vendor_management_fcl )
SELECT 
mrtpc.id as id_tarif_port_cost, 
id_ct_kind,
CASE
	WHEN TRIM(CAST(vendor_management_fcl AS VARCHAR))<>'' THEN ROUND(CAST(vendor_management_fcl as NUMERIC)*($str_rate), 2)
	ELSE 0
END as vendor_management_fcl 
FROM ref_tarifs_ports_costs_fcl_vendor_management as rtpcf
LEFT JOIN ref_tarifs_ports_costs as rtpc
ON rtpcf.id_tarif_port_cost = rtpc.id
LEFT JOIN 
(
  SELECT MAX(id) as id, id_ref_tarifs_port
  FROM ref_tarifs_ports_costs
  GROUP BY id_ref_tarifs_port
)
as mrtpc
ON mrtpc.id_ref_tarifs_port = rtpc.id_ref_tarifs_port

WHERE rtpc.id IN (
  SELECT MAX(id)
  FROM ref_tarifs_ports_costs as rtpc
  WHERE rtpc.effective_date <> '$effective_date'
  AND rtpc.effective_date < '$effective_date'
  GROUP BY id_ref_tarifs_port
);
";

my $rs_insert_fcl_vendor_management = $dbh->prepare( $sqlr_insert_fcl_vendor_management );
print $sqlr_insert_fcl_vendor_management if($dbug);
$rs_insert_fcl_vendor_management->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_fcl_vendor_management;
  $rs_insert_fcl_vendor_management->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_fcl_vendor_management->finish;

my $sqlr_insert_ports_costs_thc_air = "

INSERT INTO ref_tarifs_ports_costs_thc_air ( id_tarif_port_cost, thc_air_greater_than, thc_air_cost_per_kgs )
SELECT 
mrtpc.id as id_tarif_port_cost, 
thc_air_greater_than,
CASE
	WHEN TRIM(CAST(rtpct.thc_air_cost_per_kgs AS VARCHAR))<>'' THEN ROUND(CAST(rtpct.thc_air_cost_per_kgs as NUMERIC)*($str_rate), 2)
	ELSE 0
END as thc_air_cost_per_kgs
FROM ref_tarifs_ports_costs_thc_air as rtpct
LEFT JOIN ref_tarifs_ports_costs as rtpc
ON rtpct.id_tarif_port_cost = rtpc.id
LEFT JOIN 
(
  SELECT MAX(id) as id, id_ref_tarifs_port
  FROM ref_tarifs_ports_costs
  GROUP BY id_ref_tarifs_port
)
as mrtpc
ON mrtpc.id_ref_tarifs_port = rtpc.id_ref_tarifs_port

WHERE rtpc.id IN (
  SELECT MAX(id)
  FROM ref_tarifs_ports_costs as rtpc
  WHERE rtpc.effective_date <> '$effective_date'
  AND rtpc.effective_date < '$effective_date'
  GROUP BY id_ref_tarifs_port
);
";
my $rs_insert_ports_costs_thc_air = $dbh->prepare( $sqlr_insert_ports_costs_thc_air );
print $sqlr_insert_ports_costs_thc_air if($dbug);
$rs_insert_ports_costs_thc_air->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_ports_costs_thc_air;
  $rs_insert_ports_costs_thc_air->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_ports_costs_thc_air->finish;


my $sqlr_insert_truckings_cost_by_date = "

----------------
-- MAJ HAULAGE
----------------

INSERT INTO ref_truckings_cost_by_date (
id_trucking_cost,
effective_date,
fcl_cost_per_ct,
lcl_cost_per_cbm, 
fcl_cost_per_ct_refer,
lcl_cost_per_cbm_refer
)

SELECT 
id_trucking_cost, 
'$effective_date' as effective_date, 
CASE 
	WHEN TRIM(CAST(rtc.fcl_cost_per_ct AS VARCHAR)) <> '' THEN ROUND(CAST(rtc.fcl_cost_per_ct AS NUMERIC)*($str_rate), 2)
	ELSE 0
END as fcl_cost_per_ct , 
CASE 
	WHEN TRIM(CAST(rtc.lcl_cost_per_cbm AS VARCHAR)) <> '' THEN ROUND(CAST(rtc.lcl_cost_per_cbm AS NUMERIC)*($str_rate), 2)
	ELSE 0
END as lcl_cost_per_cbm,
CASE 
	WHEN TRIM(CAST(rtc.fcl_cost_per_ct_refer AS VARCHAR)) <> '' THEN ROUND(CAST(rtc.fcl_cost_per_ct_refer AS NUMERIC)*($str_rate), 2)
	ELSE 0
END as fcl_cost_per_ct_refer , 
CASE 
	WHEN TRIM(CAST(rtc.lcl_cost_per_cbm_refer AS VARCHAR)) <> '' THEN ROUND(CAST(rtc.lcl_cost_per_cbm_refer AS NUMERIC)*($str_rate), 2)
	ELSE 0
END as lcl_cost_per_cbm_refer

FROM ref_truckings as rt
LEFT JOIN ref_truckings_cost_by_date as rtc
ON rt.id = rtc.id_trucking_cost

WHERE CAST(rt.id AS VARCHAR)||CAST(rtc.effective_date AS VARCHAR)
IN (
SELECT
CAST(rt.id AS VARCHAR)||CAST(MAX(rtc.effective_date) AS VARCHAR)
FROM ref_truckings as rt
LEFT JOIN ref_truckings_cost_by_date as rtc
ON rt.id = rtc.id_trucking_cost
WHERE rtc.effective_date < '$effective_date'
GROUP BY rt.id
);
";
my $rs_insert_truckings_cost_by_date = $dbh->prepare( $sqlr_insert_truckings_cost_by_date );
print $sqlr_insert_truckings_cost_by_date if($dbug);
$rs_insert_truckings_cost_by_date->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_truckings_cost_by_date;
  $rs_insert_truckings_cost_by_date->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_truckings_cost_by_date->finish;

my $sqlr_insert_palletizations_costs = "

----------------
-- MAJ WAREHOUSE
----------------

INSERT INTO ref_palletizations_costs (
id_palletization,
effective_date,
stripping_ct
)

SELECT 
id_palletization, 
'$effective_date' as effective_date, 
CASE 
	WHEN TRIM(CAST(rpc.stripping_ct AS VARCHAR)) <> '' THEN ROUND(CAST(rpc.stripping_ct AS NUMERIC)*($str_rate), 2)
	ELSE 0
END as stripping_ct 

FROM ref_palletizations as rp
LEFT JOIN ref_palletizations_costs as rpc
ON rp.id = rpc.id_palletization

WHERE CAST(rp.id AS VARCHAR)||CAST(rpc.effective_date AS VARCHAR)
IN (
SELECT
CAST(rp.id AS VARCHAR)||CAST(MAX(rpc.effective_date) AS VARCHAR)
FROM ref_palletizations as rp
LEFT JOIN ref_palletizations_costs as rpc
ON rp.id = rpc.id_palletization
WHERE rpc.effective_date < '$effective_date'
GROUP BY rp.id
);

";

my $rs_insert_palletizations_costs = $dbh->prepare( $sqlr_insert_palletizations_costs );
print $sqlr_insert_palletizations_costs if($dbug);
$rs_insert_palletizations_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_palletizations_costs;
  $rs_insert_palletizations_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_palletizations_costs->finish;

my $sqlr_insert_palletizations_costs_by_pallet = "
INSERT INTO ref_palletizations_costs_by_pallet ( id_palletization_cost, id_pallet_kind, cost_per_pallet )
SELECT 
mrpc.id as id_palletization_cost, 
id_pallet_kind,
CASE
	WHEN TRIM(CAST(rpcb.cost_per_pallet AS VARCHAR))<>'' THEN ROUND(CAST(rpcb.cost_per_pallet as NUMERIC)*($str_rate), 2)
	ELSE 0
END as cost_per_pallet 

FROM ref_palletizations_costs_by_pallet as rpcb

LEFT JOIN ref_palletizations_costs as rpc
ON rpcb.id_palletization_cost = rpc.id
LEFT JOIN (
  SELECT MAX(id) as id, id_palletization
  FROM ref_palletizations_costs
  GROUP BY id_palletization
) as mrpc
ON mrpc.id_palletization = rpc.id_palletization

WHERE rpc.id IN (
  SELECT MAX(id)
  FROM ref_palletizations_costs as rpc
  WHERE rpc.effective_date <> '$effective_date'
  AND rpc.effective_date < '$effective_date'
  GROUP BY id_palletization
);

";

my $rs_insert_palletizations_costs_by_pallet = $dbh->prepare( $sqlr_insert_palletizations_costs_by_pallet );
print $sqlr_insert_palletizations_costs_by_pallet if($dbug);
$rs_insert_palletizations_costs_by_pallet->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_palletizations_costs_by_pallet;
  $rs_insert_palletizations_costs_by_pallet->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_palletizations_costs_by_pallet->finish;

my $sqlr_ins_final_truckings_costs = "

----------------
-- MAJ FND
----------------

INSERT INTO ref_final_truckings_costs (
id_final_trucking,
effective_date
)

SELECT 
id_final_trucking, 
'$effective_date' as effective_date

FROM ref_final_truckings as rf
LEFT JOIN ref_final_truckings_costs as rfc
ON rf.id = rfc.id_final_trucking

WHERE CAST(rf.id AS VARCHAR)||CAST(rfc.effective_date AS VARCHAR)
IN (
	SELECT
	CAST(rf.id AS VARCHAR)||CAST(MAX(rfc.effective_date) AS VARCHAR)
	FROM ref_final_truckings as rf
	LEFT JOIN ref_final_truckings_costs as rfc
	ON rf.id = rfc.id_final_trucking
	WHERE rfc.effective_date < '$effective_date'
	GROUP BY rf.id
);

";

my $rs_ins_final_truckings_costs = $dbh->prepare( $sqlr_ins_final_truckings_costs );
print $sqlr_ins_final_truckings_costs if($dbug);
$rs_ins_final_truckings_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_ins_final_truckings_costs;
  $rs_ins_final_truckings_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_ins_final_truckings_costs->finish;

my $sqlr_ins_truckings_cost_by_nb_pallets = "
INSERT INTO ref_final_truckings_cost_by_nb_pallets ( id_final_trucking_cost, nb_pallets, cost )
SELECT 
mrftc.id as id_final_trucking_cost, 
nb_pallets,
CASE
	WHEN TRIM(CAST(rftcb.cost AS VARCHAR))<>'' THEN ROUND(CAST(rftcb.cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as cost 

FROM ref_final_truckings_cost_by_nb_pallets as rftcb

LEFT JOIN ref_final_truckings_costs as rftc
ON rftcb.id_final_trucking_cost = rftc.id
LEFT JOIN 
(
  SELECT MAX(id) as id, id_final_trucking
  FROM ref_final_truckings_costs
  GROUP BY id_final_trucking
)
as mrftc
ON mrftc.id_final_trucking = rftc.id_final_trucking

WHERE rftc.id IN (
  SELECT MAX(id)
  FROM ref_final_truckings_costs as rftc
  WHERE rftc.effective_date <> '$effective_date'
  AND rftc.effective_date < '$effective_date'
  GROUP BY id_final_trucking
);
";

my $rs_ins_truckings_cost_by_nb_pallets = $dbh->prepare( $sqlr_ins_truckings_cost_by_nb_pallets );
print $sqlr_ins_truckings_cost_by_nb_pallets if($dbug);
$rs_ins_truckings_cost_by_nb_pallets->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_ins_truckings_cost_by_nb_pallets;
  $rs_ins_truckings_cost_by_nb_pallets->finish;
  $dbh->disconnect;
  exit;
}
$rs_ins_truckings_cost_by_nb_pallets->finish;

my $sqlr_insert_global_costs = "

----------------
-- MAJ GLOBAL
----------------

INSERT INTO ref_global_costs (
effective_date, 
inland_haulage_fuel_surcharge, 
final_trucking_fuel_surcharge, 
transport_insurance, 
transport_cif_insurance, 
lcl_freight_baf_mincbm, 
lcl_thc_mincbm, 
lcl_vm_mincbm, 
fri_cost, 
fri_edge, 
letter_credit, 
docs_mail, 
other_admin_cost, 
import_dpt_fees_rate, 
qc_dpt_fees_rate, 
d_transport_insurance, 
import_risk_fees_rate, 
purchase_fees_rate, 
other_fees_rate
)

SELECT 
'$effective_date' as effective_date, 
inland_haulage_fuel_surcharge, 
final_trucking_fuel_surcharge, 
transport_insurance, 
transport_cif_insurance, 
lcl_freight_baf_mincbm, 
lcl_thc_mincbm, 
lcl_vm_mincbm, 
CASE
	WHEN TRIM(CAST(rgc.fri_cost AS VARCHAR))<>'' THEN ROUND(CAST(rgc.fri_cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as fri_cost, 
fri_edge, 
CASE
	WHEN TRIM(CAST(rgc.letter_credit AS VARCHAR))<>'' THEN ROUND(CAST(rgc.letter_credit as NUMERIC)*($str_rate), 2)
	ELSE 0
END as letter_credit, 
CASE
	WHEN TRIM(CAST(rgc.docs_mail AS VARCHAR))<>'' THEN ROUND(CAST(rgc.docs_mail as NUMERIC)*($str_rate), 2)
	ELSE 0
END as docs_mail, 
CASE
	WHEN TRIM(CAST(rgc.other_admin_cost AS VARCHAR))<>'' THEN ROUND(CAST(rgc.other_admin_cost as NUMERIC)*($str_rate), 2)
	ELSE 0
END as other_admin_cost, 
import_dpt_fees_rate, 
qc_dpt_fees_rate, 
d_transport_insurance, 
import_risk_fees_rate, 
purchase_fees_rate, 
other_fees_rate

FROM ref_global_costs as rgc

WHERE rgc.effective_date
IN (
	SELECT
	MAX(rgc.effective_date)
	FROM ref_global_costs as rgc
	WHERE rgc.effective_date < '$effective_date'
);

";

my $rs_insert_global_costs = $dbh->prepare( $sqlr_insert_global_costs );
print $sqlr_insert_global_costs if($dbug);
$rs_insert_global_costs->execute() if(!$dbug);
if ( $dbh->errstr ne undef ) {    # ERREUR EXECUTION SQL
  print "\nError : ".$dbh->errstr."\n". $sqlr_insert_global_costs;
  $rs_insert_global_costs->finish;
  $dbh->disconnect;
  exit;
}
$rs_insert_global_costs->finish;
print $sqlr_insert_global_costs if($dbug);

#----------------
#-- MAJ MIN MAX CUSTOMS
#----------------

my $sqlr = "
			SELECT 
			concat(nh.fic_num_root||';'), 
			nr.min_custom_duties, 
			nr.max_custom_duties
			FROM nomenclature_rates as nr
			LEFT JOIN nomenclature_header as nh
			ON nr.id_nomenclature = nh.id
			WHERE 1=1
			--AND  nh.id IS NULL
			GROUP BY 
			nr.min_custom_duties, 
			nr.max_custom_duties
			;
		";
		
my $rs = $dbh->prepare($sqlr);
$rs->execute();

if ( $dbh->errstr ne undef ) {
	print $dbh->errstr.":<br><pre>".$sqlr; 
	$rs->finish;
	exit;
}


while (  my $data = $rs->fetchrow_hashref ) {
	my $current_min_custom_duties =  $data->{'min_custom_duties'};
	my $current_max_custom_duties =  $data->{'max_custom_duties'};
	
	my $sqlr2 = "
	UPDATE nomenclature_rates SET 
	min_custom_duties = ROUND(CAST('$current_min_custom_duties' AS NUMERIC)*($str_rate), 5), 
	max_custom_duties = ROUND(CAST('$current_max_custom_duties' AS NUMERIC)*($str_rate), 5)
	WHERE min_custom_duties = '$current_min_custom_duties' AND max_custom_duties ='$current_max_custom_duties' 
	; 
	";
		
	print $sqlr;
	
	my $rs = $dbh->prepare($sqlr2);
#	$rs->execute();

	if ( $dbh->errstr ne undef ) {
		print $dbh->errstr.":<br><pre>".$sqlr2; 
		$rs->finish;
		exit;
	}
	
    $rs->finish;
}

$rs->finish;
$dbh->disconnect;

