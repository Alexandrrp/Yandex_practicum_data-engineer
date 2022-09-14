drop table if exists shipping_country_rates;
drop table if exists shipping_agreement;
drop table if exists shipping_transfer;
drop table if exists shipping_info;
drop table if exists shipping_status;

--shipping_country_rates
create table public.shipping_country_rates (
shipping_country_id serial,
shipping_country text,
shipping_country_base_rate numeric(14,2),
PRIMARY KEY (shipping_country_id)
);

--shipping_agreement
create table public.shipping_agreement (
agreementid int8,
agreement_number text,
agreement_rate numeric(14,3),
agreement_commission numeric(14,3),
PRIMARY KEY (agreementid)
);

--shipping_transfer
create table public.shipping_transfer (
transfer_type_id serial,
transfer_type text,
transfer_model text,
shipping_transfer_rate numeric(14,3),
PRIMARY KEY (transfer_type_id)
);

--shipping_info
create table public.shipping_info (
shippingid bigint,
vendorid bigint,
shipping_country_id int8,
agreementid int8,
transfer_type_id int8,
shipping_plan_datetime timestamp,
payment_amount numeric(14,3),
PRIMARY KEY (shippingid),
CONSTRAINT fk_shipping_country_id FOREIGN KEY (shipping_country_id) references public.shipping_country_rates (shipping_country_id) on update cascade,
CONSTRAINT fk_agreementid FOREIGN KEY (agreementid) references public.shipping_agreement (agreementid) on update cascade,
CONSTRAINT fk_transfer_type_id FOREIGN KEY (transfer_type_id) references public.shipping_transfer (transfer_type_id) on update cascade
);

--shipping_status
create table public.shipping_status (
shippingid bigint,
status text,
state text,
shipping_start_fact_datetime timestamp,
shipping_end_fact_datetime timestamp,
PRIMARY KEY (shippingid)
); 

--shipping_country_rates
INSERT INTO public.shipping_country_rates (shipping_country, shipping_country_base_rate)
SELECT 
	DISTINCT shipping_country,
	shipping_country_base_rate
FROM public.shipping;

--shipping_agreement
INSERT INTO public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT 
	CAST(vendor_agreement_description[1] AS int8) AS agreementid,
	CAST(vendor_agreement_description[2] AS text) AS agreement_number,
	CAST(vendor_agreement_description[3] AS numeric(14,3)) AS agreement_rate,
	CAST(vendor_agreement_description[4] AS numeric(14,3)) AS agreement_commission
FROM (SELECT 
		DISTINCT regexp_split_to_array(vendor_agreement_description, ':+') AS vendor_agreement_description
	 FROM public.shipping) AS subq
ORDER BY agreementid;

--shipping_transfer
INSERT INTO public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT 
	CAST(shipping_transfer_description[1] AS TEXT) transfer_type,
	CAST(shipping_transfer_description[2] AS TEXT) transfer_model,
	CAST(shipping_transfer_rate AS numeric(14,3))  
FROM (SELECT 
		DISTINCT regexp_split_to_array(shipping_transfer_description, ':+') AS shipping_transfer_description,
		shipping_transfer_rate
	 FROM public.shipping) AS subq;

--shipping_info
INSERT INTO public.shipping_info (shippingid, vendorid, shipping_country_id,
agreementid, transfer_type_id, shipping_plan_datetime, payment_amount)
SELECT 
	DISTINCT shippingid,
	vendorid,
	r.shipping_country_id,
	a.agreementid, 
	t.transfer_type_id, 
	shipping_plan_datetime, 
	payment_amount
FROM (SELECT
		shippingid,
		vendorid, 
		shipping_plan_datetime, 
		payment_amount,
		regexp_split_to_array(vendor_agreement_description, ':+') AS vendor_agreement_description,
		regexp_split_to_array(shipping_transfer_description, ':+') AS shipping_transfer_description,
		shipping_country AS shipping_country
	 FROM public.shipping) AS s
JOIN shipping_country_rates AS r ON s.shipping_country=r.shipping_country
JOIN shipping_agreement AS a ON a.agreementid=CAST(s.vendor_agreement_description[1] AS int8)
JOIN shipping_transfer AS t ON t.transfer_type=CAST(shipping_transfer_description[1] AS TEXT)
AND t.transfer_model=CAST(shipping_transfer_description[2] AS TEXT) 
ORDER BY shippingid;

--shipping_status
INSERT INTO public.shipping_status (shippingid, status, 
state, shipping_start_fact_datetime, shipping_end_fact_datetime)
SELECT 
	DISTINCT shippingid, 
	status, 
	state, 
	shipping_start_fact_datetime, 
	shipping_end_fact_datetime 
FROM (SELECT 
		shippingid, 
		FIRST_VALUE(state_datetime) OVER(PARTITION BY shippingid ORDER BY state_datetime) AS shipping_start_fact_datetime, 
		FIRST_VALUE(state_datetime) OVER w AS shipping_end_fact_datetime, 
		FIRST_VALUE(status) OVER w AS status, 
		FIRST_VALUE(state) OVER w AS state 
	FROM public.shipping 
	WINDOW w AS (PARTITION BY shippingid ORDER BY state_datetime DESC) 
) AS shipping_status_subq; 

--view shipping_datamart
CREATE VIEW shipping_datamart AS 
SELECT 
	ss.shippingid, 
	vendorid, 
	st.transfer_type, 
	DATE_PART('day', AGE(shipping_end_fact_datetime, shipping_start_fact_datetime)) AS full_day_at_shipping, 
	shipping_end_fact_datetime > shipping_plan_datetime AS is_delay, 
	status = 'finished' AS is_shipping_finish, 
	CASE 
		WHEN shipping_end_fact_datetime > shipping_plan_datetime 
		THEN DATE_PART('day', AGE(shipping_end_fact_datetime, shipping_plan_datetime)) 
		ELSE 0 
	END AS delay_day_at_shipping, 
	payment_amount, 
	payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat, 
	payment_amount * agreement_commission AS profit 
FROM public.shipping_status ss 
JOIN public.shipping_info si ON ss.shippingid = si.shippingid 
JOIN public.shipping_transfer st ON si.transfer_type_id = st.transfer_type_id 
JOIN public.shipping_country_rates scr ON scr.shipping_country_id = si.shipping_country_id 
JOIN public.shipping_agreement sa ON si.agreementid = sa.agreementid; 
 