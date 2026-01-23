    contacten_elimineren AS
    (
    SELECT DISTINCT ON (patient_id, datum, contactsoort, medewerker_id, icpc)
            patient_id,
            datum,
            medewerker_id,
            icpc,
            contactsoort
        FROM icpc_context
        WHERE contactsoort SIMILAR TO '(C|V|T|E)%'
		AND (icpc NOT ILIKE '%R44%' OR icpc IS NULL)
        ORDER BY patient_id NULLS LAST,
                datum NULLS LAST,
                contactsoort NULLS LAST,
                medewerker_id NULLS LAST,
                icpc NULLS LAST
    ),
    contacten_nummering AS
    (
        SELECT row_number() over (partition by patient_id,
                                    medewerker_id,
                                    contactsoort,
                                    datum
                              order by icpc nulls last) as nr,
            patient_id,
            datum,
            medewerker_id,
            icpc,
            contactsoort
        FROM contacten_elimineren
    ),
    contacten_filter AS
    (
    SELECT
        patient_id,
        datum,
        medewerker_id,
        contactsoort,
        icpc,
        COALESCE(i.description, 'ICPC code onbekend') as icpc_omschrijving
    FROM contacten_nummering c
    LEFT JOIN indelingen.icpc_description i ON c.icpc = i.code
    WHERE c.icpc IS NOT NULL
        OR nr = 1
    ORDER BY patient_id, datum, contactsoort, icpc, medewerker_id
    ),    
	contacten_dubbel AS
	(
	SELECT *, row_number() over (partition by patient_id, 
							  	datum, 
							  	contactsoort, 
							  	medewerker_id, 
							  	LEFT(icpc, 3) 
							  order by icpc desc) as nr_dubbel,
		-- Dubbele contacten met allebei een andere subcode willen we wel meetellen, daarom hoofdcode gedefinieerd
		-- Zodat deze in de view hierna gebruikt kan worden om alle subcodes nog wel mee te nemen.
		CASE WHEN length(icpc)=3 THEN 1 ELSE 0 END as hoofdcode
	FROM contacten_filter
	)
	SELECT
        patient_id,
        datum,
        medewerker_id,
        contactsoort,
        CASE WHEN contactsoort SIMILAR TO '(E|C:EC|T|C:TD|CK|C:CH)%' THEN 0.5
                 WHEN contactsoort SIMILAR TO '(C:C2|C:CL|C:DC)%' THEN 2
                 WHEN contactsoort SIMILAR TO '(C)%' THEN 1
                 WHEN contactsoort SIMILAR TO '(V:V2|V:VL)%' THEN 2.5
                 WHEN contactsoort SIMILAR TO '(V)%' THEN 1.5
            ELSE NULL END as contact_zorgzwaarte,
        icpc,
        icpc_omschrijving
	FROM contacten_dubbel
	WHERE nr_dubbel=1 OR hoofdcode=0