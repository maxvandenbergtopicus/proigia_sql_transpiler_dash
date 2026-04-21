 WITH 
    {% include 'proigia_basis_meetwaarden_cns.pry' %},
    pre_verzamel AS
    (
    SELECT
        patient_id,
        patientnummer,
        bsn,
        achternaam,
        tussenvoegsel,
        achternaam_partner,
        tussenvoegsel_partner,
        voorletters,
        geboortedatum,
        aanschrijfnaam,
        straatnaam,
        huisnummer,
        huisnummer_toevoeging,
        postcode,
        woonplaats,
        inschrijfdatum,
        naam_zorgverzekeraar,
        uzovi,
        polisnummer,
        huisarts,
        leeftijd,
        geslacht,
        naam,
        telefoonnummer,
        emailadres,
        geboortemaand,
        COALESCE(apotheek, a.gebruikersnaam) AS apotheek,
        COALESCE(CAST(a05_[2] AS INT),0) AS a05,
        COALESCE(CAST(t90[2] AS INT), 0) AS t90_hoofd,
        COALESCE(CAST(t90_[2] AS INT),0) AS dm,
        COALESCE(CAST(t90_01[2] AS INT), 0) AS dm1,
        COALESCE(CAST(t90_02[2] AS INT), 0) AS dm2,
        COALESCE(CAST(k49_01[2] AS INT),0) AS k49_01,
        COALESCE(CAST(k86_[2] AS INT), 0) AS k86,
        COALESCE(CAST(k87_[2] AS INT), 0) AS k87,
        COALESCE(CAST(l88_[2] AS INT),0) AS l88,
        COALESCE(CAST(u05_02[2] AS INT),0) AS u05_02,
        COALESCE(CAST(u75_[2] AS INT), 0) AS u75,
        COALESCE(CAST(u76_[2] AS INT), 0) AS u76,
        COALESCE(CAST(u77_[2] AS INT), 0) AS u77,
        COALESCE(CAST(u79_[2] AS INT), 0) AS u79,
        COALESCE(CAST(u80[2] AS INT), 0) AS u80_hoofd,
        COALESCE(CAST(u80_01[2] AS INT), 0) AS u80_01,
        COALESCE(CAST(u80_02[2] AS INT), 0) AS u80_02,
        COALESCE(CAST(u85_[2] AS INT), 0) AS u85,
        COALESCE(CAST(u85[2] AS INT), 0) AS u85_hoofd,
        COALESCE(CAST(u85_01[2] AS INT), 0) AS u85_01,
        COALESCE(CAST(u88_[2] AS INT), 0) AS u88,
        COALESCE(CAST(u98_01[2] AS INT), 0) AS u98_01,
        COALESCE(CAST(u98_03[2] AS INT), 0) AS u98_03,
        COALESCE(CAST(u99_[2] AS INT), 0) AS u99,
        COALESCE(CAST(u99[2] AS INT), 0) AS u99_hoofd,
        COALESCE(CAST(u99_01[2] AS INT), 0) AS u99_01,
        COALESCE(CAST(u99_02[2] AS INT), 0) AS u99_02,
        COALESCE(CAST(u99_03[2] AS INT), 0) AS u99_03,
        COALESCE(CAST(u99_04[2] AS INT), 0) AS u99_04,
        CASE WHEN COALESCE(CAST(u75_[2] AS INT), 0)+
                  COALESCE(CAST(u76_[2] AS INT), 0)+
                  COALESCE(CAST(u77_[2] AS INT), 0)+
                  COALESCE(CAST(u79_[2] AS INT), 0)+
                  COALESCE(CAST(u80[2] AS INT), 0)+
                  COALESCE(CAST(u80_01[2] AS INT), 0)+
                  COALESCE(CAST(u80_02[2] AS INT), 0)+
                  COALESCE(CAST(u85[2] AS INT), 0)+
                  COALESCE(CAST(u85_01[2] AS INT), 0)+
                  COALESCE(CAST(u88_[2] AS INT), 0)+
                  COALESCE(CAST(u98_01[2] AS INT), 0)+
                  COALESCE(CAST(u98_03[2] AS INT), 0)+
                  COALESCE(CAST(u99[2] AS INT), 0)+
                  COALESCE(CAST(u99_01[2] AS INT), 0)+
                  COALESCE(CAST(u99_02[2] AS INT), 0)+
                  COALESCE(CAST(u99_03[2] AS INT), 0)+
                  COALESCE(CAST(u99_04[2] AS INT), 0)>0 THEN 1 ELSE 0 END AS icpc_urinewegen,
        CASE WHEN COALESCE(CAST(u75_[2] AS INT), 0)+
                  COALESCE(CAST(u76_[2] AS INT), 0)+
                  COALESCE(CAST(u77_[2] AS INT), 0)+
                  COALESCE(CAST(u79_[2] AS INT), 0)+
                  COALESCE(CAST(u80[2] AS INT), 0)+
                  COALESCE(CAST(u80_01[2] AS INT), 0)+
                  COALESCE(CAST(u80_02[2] AS INT), 0)+
                  COALESCE(CAST(u85[2] AS INT), 0)+
                  COALESCE(CAST(u85_01[2] AS INT), 0)+
                  COALESCE(CAST(u88_[2] AS INT), 0)+
                  COALESCE(CAST(u98_01[2] AS INT), 0)+
                  COALESCE(CAST(u98_03[2] AS INT), 0)+
                  COALESCE(CAST(u99_02[2] AS INT), 0)+
                  COALESCE(CAST(u99_03[2] AS INT), 0)+
                  COALESCE(CAST(u99_04[2] AS INT), 0)>0 THEN 1 ELSE 0 END AS icpc_urinewegen_tbv_screening,
        CASE WHEN COALESCE(CAST(u75_[2] AS INT), 0)+
                  COALESCE(CAST(u76_[2] AS INT), 0)+
                  COALESCE(CAST(u77_[2] AS INT), 0)+
                  COALESCE(CAST(u79_[2] AS INT), 0)+
                  COALESCE(CAST(u85_[2] AS INT), 0)+
                  COALESCE(CAST(u88_[2] AS INT), 0)+
                  COALESCE(CAST(u99_[2] AS INT), 0)>0 THEN 1 ELSE 0 END AS icpc_verlaagd_egfr,
        CASE WHEN ((coalesce(CAST(k86_[2] AS INT),0)=1 AND coalesce(CAST(c02[0] AS INTEGER),0)+coalesce(CAST(c03[0] AS INTEGER),0)+coalesce(CAST(c07[0] AS INTEGER),0)+coalesce(CAST(c08[0] AS INTEGER),0)+coalesce(CAST(c09[0] AS INTEGER),0)>0)
            OR (coalesce(CAST(k87_[2] AS INT),0)=1 AND coalesce(CAST(c02[0] AS INTEGER),0)+coalesce(CAST(c03[0] AS INTEGER),0)+coalesce(CAST(c07[0] AS INTEGER),0)+coalesce(CAST(c08[0] AS INTEGER),0)+coalesce(CAST(c09[0] AS INTEGER),0)>0)
            OR (coalesce(CAST(t93_[2] AS INT),0)=1 AND coalesce(CAST(c10[0] AS INTEGER),0)>0)
            OR (COALESCE(rh_rood_geel_eerste_meting,0)=1 AND leeftijd>=0 AND leeftijd<=70))
            THEN 1 ELSE 0 END AS vvr,
        CASE WHEN (COALESCE(CAST(k74_[2] AS INT), 0)+
                   COALESCE(CAST(k75_[2] AS INT), 0)+
                   COALESCE(CAST(k76_[2] AS INT), 0)+
                   COALESCE(CAST(k89_[2] AS INT), 0)+
                   COALESCE(CAST(k90[2] AS INT), 0)+
                   COALESCE(CAST(k90_02[2] AS INT), 0)+
                   COALESCE(CAST(k90_03[2] AS INT), 0)+
                   COALESCE(CAST(k91_[2] AS INT), 0)+
                   COALESCE(CAST(k92_01[2] AS INT), 0)+
                   COALESCE(CAST(k99_01[2] AS INT), 0))>0 
            THEN 1 ELSE 0 END as hvz,
        CAST(admi_aq[2] AS VARCHAR) AS admi_aq,
        TRY_TO_DATE(TO_VARCHAR(admi_aq[1])) AS admi_aq_laatste_dd,
        CAST(alb_u[0] AS VARCHAR) AS alb_u,
        TRY_TO_DATE(TO_VARCHAR(alb_u[1])) AS alb_u_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(alb_u[2])) AS alb_u_num,
        CAST(alb_b[0] AS VARCHAR) AS alb_b,
        TRY_TO_DATE(TO_VARCHAR(alb_b[1])) AS alb_b_laatste_dd,
        CAST(alb_ue_mt[0] AS VARCHAR) AS alb_ue_mt,
        TRY_TO_DATE(TO_VARCHAR(alb_ue_mt[1])) AS alb_ue_mt_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(alb_ue_mt[2])) AS alb_ue_mt_num,
        CAST(albk_u_mi[0] AS VARCHAR) AS albk_u_mi,
        TRY_TO_DATE(TO_VARCHAR(albk_u_mi[1])) AS albk_u_mi_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2])) AS albk_u_mi_num,
        CAST(awkw_az[2] AS VARCHAR) AS awkw_az,
        TRY_TO_DATE(TO_VARCHAR(awkw_az[1])) AS awkw_az_laatste_dd,
        CAST(ca_b[0] AS VARCHAR) AS ca_b,
        TRY_TO_DATE(TO_VARCHAR(ca_b[1])) AS ca_b_laatste_dd,
        CAST(cvkz_kz[2] AS VARCHAR) AS cvkz_kz,
        TRY_TO_DATE(TO_VARCHAR(cvkz_kz[1])) AS cvkz_kz_laatste_dd,
        CAST(dmkz_tz[2] AS VARCHAR) AS dmkz_tz,
        TRY_TO_DATE(TO_VARCHAR(dmkz_tz[1])) AS dmkz_tz_laatste_dd,
        CAST(egfc_o_fb[0] AS VARCHAR) AS egfc_o_fb,
        TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])) AS egfc_o_fb_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2])) AS egfc_o_fb_num,
        CAST(egcc_o_fb[0] AS VARCHAR) AS egcc_o_fb,
        TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1])) AS egcc_o_fb_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2])) AS egcc_o_fb_num,
        CAST(fosf_b[0] AS VARCHAR) AS fosf_b,
        TRY_TO_DATE(TO_VARCHAR(fosf_b[1])) AS fosf_b_laatste_dd,
        CAST(fosf_u[0] AS VARCHAR) AS fosf_u,
        TRY_TO_DATE(TO_VARCHAR(fosf_u[1])) AS fosf_u_laatste_dd,
        CAST(fosf_ue_mt[0] AS VARCHAR) AS fosf_ue_mt,
        fosf_ue_mt[1]::date fosf_ue_mt_laatste_dd,
        CAST(gew_ao[0] AS VARCHAR) AS gew_ao,
        TRY_TO_DATE(TO_VARCHAR(gew_ao[1])) AS gew_ao_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(gew_ao[2])) AS gew_ao_num,
        CAST(gsta_aq[2] AS VARCHAR) AS gsta_aq,
        TRY_TO_DATE(TO_VARCHAR(gsta_aq[1])) AS gsta_aq_laatste_dd,
        CAST(hb_b[0] AS VARCHAR) AS hb_b,
        TRY_TO_DATE(TO_VARCHAR(hb_b[1])) AS hb_b_laatste_dd,
        CAST(k_b[0] AS VARCHAR) AS k_b,
        TRY_TO_DATE(TO_VARCHAR(k_b[1])) AS k_b_laatste_dd,
        CAST(k_u[0] AS VARCHAR) AS k_u,
        TRY_TO_DATE(TO_VARCHAR(k_u[1])) AS k_u_laatste_dd,
        CAST(krea_b[0] AS VARCHAR) AS krea_b,
        TRY_TO_DATE(TO_VARCHAR(krea_b[1])) AS krea_b_laatste_dd,
        CAST(krea_o_mk[0] AS VARCHAR) AS krea_o_mk,
        TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])) AS krea_o_mk_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2])) AS krea_o_mk_num,
        CAST(krea_ue_mt[0] AS VARCHAR) AS krea_ue_mt,
        TRY_TO_DATE(TO_VARCHAR(krea_ue_mt[1])) AS krea_ue_mt_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(krea_ue_mt[2])) AS krea_ue_mt_num,
        CAST(ldl_b[0] AS VARCHAR) AS ldl_b,
        TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) AS ldl_b_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b[2])) AS ldl_b_num,
        CAST(ldl_b_po[0] AS VARCHAR) AS ldl_b_po,
        TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) AS ldl_b_po_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b_po[2])) AS ldl_b_po_num,
        CAST(ldld_b[0] AS VARCHAR) AS ldld_b, 	
        TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) AS ldld_b_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(ldld_b[2])) AS ldld_b_num,
        CAST(ldsw_bq[0] AS VARCHAR) AS ldsw_bq,
        TRY_TO_DATE(TO_VARCHAR(ldsw_bq[1])) AS ldsw_bq_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(ldsw_bq[2])) AS ldsw_bq_num,
        CAST(lngp_ao[0] AS VARCHAR) AS lngp_ao,
        TRY_TO_DATE(TO_VARCHAR(lngp_ao[1])) AS lngp_ao_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2])) AS lngp_ao_num,
        CAST(na_b[0] AS VARCHAR) AS na_b,
        TRY_TO_DATE(TO_VARCHAR(na_b[1])) AS na_b_laatste_dd,
        CAST(pth_b[0] AS VARCHAR) AS pth_b,
        TRY_TO_DATE(TO_VARCHAR(pth_b[1])) AS pth_b_laatste_dd,
        CAST(quet_ao[0] AS VARCHAR) AS quet_ao,
        TRY_TO_DATE(TO_VARCHAR(quet_ao[1])) AS quet_ao_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(quet_ao[2])) AS quet_ao_num,
        CAST(krem_o_fb[0] AS VARCHAR) AS krem_o_fb,
        krem_o_fb[1]::date krem_o_fb_laatste_dd,
        CAST(kwbh_az[2] AS VARCHAR) AS kwbh_az,
        TRY_TO_DATE(TO_VARCHAR(kwbh_az[1])) AS kwbh_az_laatste_dd,
        CAST(kwcz_az[2] AS VARCHAR) AS kwcz_az,
        TRY_TO_DATE(TO_VARCHAR(kwcz_az[1])) AS kwcz_az_laatste_dd,
        CAST(rcns_un_fb[2] AS VARCHAR) AS rcns_un_fb,
        TRY_TO_DATE(TO_VARCHAR(rcns_un_fb[1])) AS rcns_un_fb_laatste_dd,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(rook_aq[1])),TRY_TO_DATE(TO_VARCHAR(rost_aq[1])))=TRY_TO_DATE(TO_VARCHAR(rook_aq[1]))
             THEN CAST(rook_aq[2] AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(rook_aq[1])),TRY_TO_DATE(TO_VARCHAR(rost_aq[1])))=TRY_TO_DATE(TO_VARCHAR(rost_aq[1]))
             THEN CASE WHEN CAST(rost_aq[2] AS VARCHAR) ILIKE 'roker' THEN 'ja'
                       WHEN CAST(rost_aq[2] AS VARCHAR) ILIKE 'stopper%' OR CAST(rost_aq[2] AS VARCHAR) ILIKE 'ex-roker%' THEN 'voorheen'
                       WHEN CAST(rost_aq[2] AS VARCHAR) ILIKE 'nooit-roker' THEN 'nooit'
               ELSE CAST(rost_aq[2] AS VARCHAR) END
            ELSE NULL END AS laatste_roken,
        GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(rook_aq[1])),TRY_TO_DATE(TO_VARCHAR(rost_aq[1]))) AS laatste_roken_laatste_dd,
        CAST(rr3s_ka[0] AS VARCHAR) AS rr3s_ka,
        TRY_TO_DATE(TO_VARCHAR(rr3s_ka[1])) AS rr3s_ka_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rr3s_ka[2])) AS rr3s_ka_num,
        CAST(rrgd_ka_mh[0] AS VARCHAR) AS rrgd_ka_mh,
        TRY_TO_DATE(TO_VARCHAR(rrgd_ka_mh[1])) AS rrgd_ka_mh_laatste_dd,
        CAST(rrgs_ka_mh[0] AS VARCHAR) AS rrgs_ka_mh,
        TRY_TO_DATE(TO_VARCHAR(rrgs_ka_mh[1])) AS rrgs_ka_mh_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrgs_ka_mh[2])) AS rrgs_ka_mh_num,
        CAST(rrs7_ka_mh[0] AS VARCHAR) AS rrs7_ka_mh,
        TRY_TO_DATE(TO_VARCHAR(rrs7_ka_mh[1])) AS rrs7_ka_mh_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrs7_ka_mh[2])) AS rrs7_ka_mh_num,
        CAST(rrdi_ka[0] AS VARCHAR) AS rrdi_ka,
        TRY_TO_DATE(TO_VARCHAR(rrdi_ka[1])) AS rrdi_ka_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrdi_ka[2])) AS rrdi_ka_num,
        CAST(rrdi_ka_mh[0] AS VARCHAR) AS rrdi_ka_mh,
        TRY_TO_DATE(TO_VARCHAR(rrdi_ka_mh[1])) AS rrdi_ka_mh_laatste_dd,
        CAST(rrsw_kq[0] AS VARCHAR) AS rrsw_kq,
        TRY_TO_DATE(TO_VARCHAR(rrsw_kq[1])) AS rrsw_kq_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrsw_kq[2])) AS rrsw_kq_num,
        CAST(rrsy_ka[0] AS VARCHAR) AS rrsy_ka,
        TRY_TO_DATE(TO_VARCHAR(rrsy_ka[1])) AS rrsy_ka_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrsy_ka[2])) AS rrsy_ka_num,
        CAST(rrsy_ka_mh[0] AS VARCHAR) AS rrsy_ka_mh,
        TRY_TO_DATE(TO_VARCHAR(rrsy_ka_mh[1])) AS rrsy_ka_mh_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(rrsy_ka_mh[2])) AS rrsy_ka_mh_num,
        CAST(advd_aq[2] AS VARCHAR) AS advd_aq,
        TRY_TO_DATE(TO_VARCHAR(advd_aq[1])) AS advd_aq_laatste_dd,
        CAST(echo_un_li[0] AS VARCHAR) AS echo_un_li,
        TRY_TO_DATE(TO_VARCHAR(echo_un_li[1])) AS echo_un_li_laatste_dd,
        CAST(echo_un_re[0] AS VARCHAR) AS echo_un_re,
        TRY_TO_DATE(TO_VARCHAR(echo_un_re[1])) AS echo_un_re_laatste_dd,
        CAST(nfhb_uz[2] AS VARCHAR) AS nfhb_uz,
        TRY_TO_DATE(TO_VARCHAR(nfhb_uz[1])) AS nfhb_uz_laatste_dd,
        COALESCE(CAST(nfhb_db[0] AS INT),0) AS dubbele_hb_cns,
        CAST(nfhb_db[2] AS VARCHAR) AS laatste_waarden_hb_cns,
        CAST(nfkz_uz[2] AS VARCHAR) AS nfkz_uz,
        TRY_TO_DATE(TO_VARCHAR(nfkz_uz[1])) AS nfkz_uz_laatste_dd,
        CAST(nfcb_uz[2] AS VARCHAR) AS nfcb_uz,
        TRY_TO_DATE(TO_VARCHAR(nfcb_uz[1])) AS nfcb_uz_laatste_dd,
        CAST(nfrz_uz[2] AS VARCHAR) AS nfrz_uz,
        TRY_TO_DATE(TO_VARCHAR(nfrz_uz[1])) AS nfrz_uz_laatste_dd,
        CAST(nhdl_b[0] AS VARCHAR) AS nhdl_b,
        TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) AS nhdl_b_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b[2])) AS nhdl_b_num,
        CAST(nhdl_b_po[0] AS VARCHAR) AS nhdl_b_po,
        TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) AS nhdl_b_po_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b_po[2])) AS nhdl_b_po_num,
        CAST(nhsw_bq[0] AS VARCHAR) AS nhsw_bq,
        TRY_TO_DATE(TO_VARCHAR(nhsw_bq[1])) AS nhsw_bq_laatste_dd,
        TRY_TO_DECFLOAT(TO_VARCHAR(nhsw_bq[2])) AS nhsw_bq_num,
        CAST(krec_o_fb[0] AS VARCHAR) AS krec_o_fb,
        krec_o_fb[1]::date krec_o_fb_laatste_dd,
        CAST(vd_b[0] AS VARCHAR) AS vd_b,
        TRY_TO_DATE(TO_VARCHAR(vd_b[1])) AS vd_b_laatste_dd,
        CAST(rh12_kq_fb[2] AS VARCHAR) AS rh12_kq_fb,
        TRY_TO_DATE(TO_VARCHAR(rh12_kq_fb[1])) AS rh12_kq_fb_laatste_dd,
        CAST(rh19_kq_fb[2] AS VARCHAR) AS rh19_kq_fb,
        TRY_TO_DATE(TO_VARCHAR(rh19_kq_fb[1])) AS rh19_kq_fb_laatste_dd,
        CAST(cvhb_kz[2] AS VARCHAR) AS cvhb_kz,
        TRY_TO_DATE(TO_VARCHAR(cvhb_kz[1])) AS cvhb_kz_laatste_dd,
        CAST(dmhb_tz[2] AS VARCHAR) AS dmhb_tz,
        TRY_TO_DATE(TO_VARCHAR(dmhb_tz[1])) AS dmhb_tz_laatste_dd,
        CAST(cysc_b[0] AS VARCHAR) AS cysc_b,
        TRY_TO_DATE(TO_VARCHAR(cysc_b[1])) AS cysc_b_laatste_dd,
        CAST(zobe_aq[2] AS VARCHAR) AS zobe_aq,
        TRY_TO_DATE(TO_VARCHAR(zobe_aq[1])) AS zobe_aq_laatste_dd,
        -- Risicocat. op basis van alleen lab
        CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<3
                AND laatste_egfr_num_std>=60
            THEN 1 ELSE 0 END AS cns_geen_lab,
        CASE WHEN (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<3
                AND laatste_egfr_num_std>=45
                AND laatste_egfr_num_std<60)
            OR (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>=3
                AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<=30
                AND laatste_egfr_num_std>=60)
            THEN 1 ELSE 0 END AS cns_mild_verhoogd_risico_lab,
        CASE WHEN (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<3
                AND laatste_egfr_num_std>=30
                AND laatste_egfr_num_std<45)
            OR (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>=3
                AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<=30
                AND laatste_egfr_num_std>=45
                AND laatste_egfr_num_std<60)
            OR (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>30
                AND laatste_egfr_num_std>=60)
            THEN 1 ELSE 0 END AS cns_matig_verhoogd_risico_lab,
        CASE WHEN (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<3
                AND laatste_egfr_num_std<30)
            OR (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>=3
                AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<=30
                AND laatste_egfr_num_std<45)
            OR (TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>30
                AND laatste_egfr_num_std<60)
            THEN 1 ELSE 0 END AS cns_sterk_verhoogd_risico_lab,
        -- STD risicocat.
        COALESCE(cns_geen_std,0) AS cns_geen_std,
        COALESCE(cns_mild_verhoogd_risico_std,0) AS cns_mild_verhoogd_risico_std,
        COALESCE(cns_matig_verhoogd_risico_std,0) AS cns_matig_verhoogd_risico_std,
        COALESCE(cns_sterk_verhoogd_risico_std,0) AS cns_sterk_verhoogd_risico_std,
        laatste_egfr_std,
        laatste_egfr_num_std,
        laatste_egfr_laatste_dd_std,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1])))=TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1])) THEN CAST(egcc_o_fb[0] AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1])))=TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])) THEN CAST(egfc_o_fb[0] AS VARCHAR)
            ELSE NULL END AS laatste_egfr_met_cyst,
        GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1]))) AS laatste_egfr_met_cyst_laatste_dd,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(krem_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])),TRY_TO_DATE(TO_VARCHAR(krec_o_fb[1])))=TRY_TO_DATE(TO_VARCHAR(krec_o_fb[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(krem_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])),TRY_TO_DATE(TO_VARCHAR(krec_o_fb[1])))=TRY_TO_DATE(TO_VARCHAR(krem_o_fb[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>0 THEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(krem_o_fb[1])),TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])),TRY_TO_DATE(TO_VARCHAR(krec_o_fb[1])))=TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))
            ELSE NULL END AS laatste_egfr_zonder_cyst_num,
        {% include 'meetwaarden_verzameltabel_seg2.pry' %},
        CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(gew_ao[2]))=0 THEN 0 ELSE CAST(ROUND(TRY_TO_DECFLOAT(TO_VARCHAR(gew_ao[2]))/((TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2])))*(TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2])))),1) AS NUMBER(36, 1)) END AS bmi_berekend_num,
        CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(quet_ao[2])) IS NOT NULL AND TRY_TO_DATE(TO_VARCHAR(quet_ao[1]))>= DATEADD(YEAR, -1, TRY_TO_DATE(TO_VARCHAR('${peildatum}'))) THEN TRY_TO_DECFLOAT(TO_VARCHAR(quet_ao[2]))
         WHEN (CAST(quet_ao[0] AS VARCHAR) IS NULL OR TRY_TO_DATE(TO_VARCHAR(quet_ao[1]))< DATEADD(YEAR, -1, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))) AND
            TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2]))>0 AND TRY_TO_DATE(TO_VARCHAR(lngp_ao[1]))>= DATEADD(YEAR, -1, TRY_TO_DATE(TO_VARCHAR('${peildatum}'))) AND
            TRY_TO_DECFLOAT(TO_VARCHAR(gew_ao[2]))>0 AND TRY_TO_DATE(TO_VARCHAR(gew_ao[1]))>= DATEADD(YEAR, -1, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))
            THEN CAST(ROUND(TRY_TO_DECFLOAT(TO_VARCHAR(gew_ao[2]))/((TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2])))*(TRY_TO_DECFLOAT(TO_VARCHAR(lngp_ao[2])))),1) AS NUMBER(36, 1))
             ELSE NULL END AS bmi_laatste_jaar_num,
        -- laatste_ldl
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) THEN CAST(nhdl_b[0] AS VARCHAR)
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) THEN CAST(nhdl_b_po[0] AS VARCHAR)
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) THEN CAST(ldl_b[0] AS VARCHAR)
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) THEN CAST(ldld_b[0] AS VARCHAR)
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) THEN CAST(ldl_b_po[0] AS VARCHAR)
            ELSE NULL END AS laatste_ldl,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b[2]))
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b_po[2]))
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b[2]))
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(ldld_b[2]))
            WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) THEN TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b_po[2]))
            ELSE NULL END AS laatste_ldl_num,
        GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1]))) AS laatste_ldl_laatste_dd,        
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) THEN CAST('nhdl_b' AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) THEN CAST('nhdl_b_po' AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) THEN CAST('ldl_b' AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) THEN CAST('ldld_b' AS VARCHAR)
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) THEN CAST('ldl_b_po' AS VARCHAR)
             ELSE NULL END AS laatste_ldl_tekst,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b[2]))<3.4 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b_po[2]))<3.4 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b[2]))<2.6 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldld_b[2]))<2.6 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b_po[2]))<2.6 THEN 1
             ELSE 0 END AS laatste_ldl_gereguleerd,
        CASE WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b[2]))<2.6 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(nhdl_b_po[2]))<2.6 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b[2]))<1.8 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldld_b[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldld_b[2]))<1.8 THEN 1
             WHEN GREATEST_IGNORE_NULLS(TRY_TO_DATE(TO_VARCHAR(ldl_b[1])),TRY_TO_DATE(TO_VARCHAR(ldld_b[1])),TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b[1])),TRY_TO_DATE(TO_VARCHAR(nhdl_b_po[1])))=TRY_TO_DATE(TO_VARCHAR(ldl_b_po[1])) AND TRY_TO_DECFLOAT(TO_VARCHAR(ldl_b_po[2]))<1.8 THEN 1
             ELSE 0 END AS laatste_ldl_gereguleerd_hvz,
        -- voorlaatste ldl       
        CASE WHEN voorlaatste_ldl_soort='non-HDL' AND voorlaatste_ldl_num<3.4 THEN 1
             WHEN voorlaatste_ldl_soort='LDL' AND voorlaatste_ldl_num<2.6 THEN 1
             ELSE 0 END AS voorlaatste_ldl_gereguleerd,
        CASE WHEN voorlaatste_ldl_soort='non-HDL' AND voorlaatste_ldl_num<2.6 THEN 1
             WHEN voorlaatste_ldl_soort='LDL' AND voorlaatste_ldl_num<1.8 THEN 1
             ELSE 0 END AS voorlaatste_ldl_gereguleerd_hvz,    
        voorlaatste_ldl_soort,
        voorlaatste_ldl_num,
        voorlaatste_ldl_datum,
        rh_laatste_uitslag,
        rh_laatste_dd,
        waarde_datum_creat_cyst,
        waarde_datum_cyst,
        waarde_datum_ckd,
        waarde_datum_mdrd,
        waarde_datum_kreat,
        coalesce(CAST(c03[0] AS INTEGER),0) AS c03_voorschriften,
        coalesce(CAST(c03[0] AS INTEGER),0)
            +coalesce(CAST(c07[0] AS INTEGER),0)
            +coalesce(CAST(c08[0] AS INTEGER),0)
            +coalesce(CAST(c09[0] AS INTEGER),0)
            +coalesce(CAST(c02a[0] AS INTEGER),0)
            +coalesce(CAST(c02l[0] AS INTEGER),0)
            +coalesce(CAST(c02ca[0] AS INTEGER),0)
            +coalesce(CAST(c02db[0] AS INTEGER),0) AS hypertensiva_voorschriften,
        coalesce(CAST(c10[0] AS INTEGER),0) AS c10_voorschriften,
        coalesce(CAST(b01a[0] AS INT),0) AS b01a_voorschriften,
        coalesce(CAST(b01ae[0] AS INT),0) AS b01ae_voorschriften,
        coalesce(CAST(b01af[0] AS INT),0) AS b01af_voorschriften,
        coalesce(CAST(m04aa01[0] AS INT),0) AS m04aa01_voorschriften,
        coalesce(CAST(c01aa[0] AS INT),0) AS c01aa_voorschriften,
        coalesce(CAST(c09[0] AS INTEGER),0) AS c09_voorschriften,
        TRY_TO_DATE(TO_VARCHAR(c09[1])) AS c09_laatste_dd,
        coalesce(CAST(c09a[0] AS INTEGER),0) AS c09a_voorschriften,
        TRY_TO_DATE(TO_VARCHAR(c09a[1])) AS c09a_laatste_dd,
        coalesce(CAST(c09b[0] AS INTEGER),0) AS c09b_voorschriften,
        TRY_TO_DATE(TO_VARCHAR(c09b[1])) AS c09b_laatste_dd,
        coalesce(CAST(c09c[0] AS INTEGER),0) AS c09c_voorschriften,
        TRY_TO_DATE(TO_VARCHAR(c09c[1])) AS c09c_laatste_dd,
        coalesce(CAST(c09d[0] AS INTEGER),0) AS c09d_voorschriften,
        TRY_TO_DATE(TO_VARCHAR(c09d[1])) AS c09d_laatste_dd,
        coalesce(ci_nier,0) AS ci_nier,
        albk_u_mi_num_3mt,
        laatste_egfr_num_3mt,
        laatste_egfr_laatste_dd_3mt,
        --creat_cyst
        CASE 
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))=0 THEN 0
            ELSE CAST(round(((TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))-TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2])))/TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2])))*100, 1) AS NUMBER(36, 1))
        END AS egfr_verloop_creat_cyst,
        CASE WHEN TRY_TO_DATE(TO_VARCHAR(egcc_o_fb_eerste[1]))=TRY_TO_DATE(TO_VARCHAR(egcc_o_fb[1])) THEN 0
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<90 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<60 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<45 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<30 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<15 THEN 5
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<60 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<45 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<30 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<15 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<45 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<15 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<15 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb_eerste[2]))<30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egcc_o_fb[2]))<15 THEN 1
        ELSE 0 END AS stijging_stadium_egfr_creat_cyst,
        CAST(egcc_o_fb_eerste[0] AS VARCHAR) AS egcc_o_fb_eerste,
        TRY_TO_DATE(TO_VARCHAR(egcc_o_fb_eerste[1])) AS egcc_o_fb_eerste_dd,
        --cyst
        CAST(round(CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))=0 THEN 0
            ELSE ((TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))-TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2])))/TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2])))*100
            END, 1) AS NUMBER(36, 1)) AS egfr_verloop_cyst,
        CASE WHEN TRY_TO_DATE(TO_VARCHAR(egfc_o_fb_eerste[1]))=TRY_TO_DATE(TO_VARCHAR(egfc_o_fb[1])) THEN 0
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<90 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<60 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<45 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<30 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<15 THEN 5
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<60 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<45 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<30 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<15 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<45 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<15 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<15 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb_eerste[2]))<30 AND TRY_TO_DECFLOAT(TO_VARCHAR(egfc_o_fb[2]))<15 THEN 1
        ELSE 0 END AS stijging_stadium_egfr_cyst,
        CAST(egfc_o_fb_eerste[0] AS VARCHAR) AS egfc_o_fb_eerste,
        TRY_TO_DATE(TO_VARCHAR(egfc_o_fb_eerste[1])) AS egfc_o_fb_eerste_dd,
        --ckd
        CAST(round(CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))=0 THEN 0
            ELSE ((TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))-TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2])))/TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2])))*100
            END, 1) AS NUMBER(36, 1)) AS egfr_verloop_ckd,
        CASE WHEN TRY_TO_DATE(TO_VARCHAR(krec_o_fb_eerste[1]))=TRY_TO_DATE(TO_VARCHAR(krec_o_fb[1])) THEN 0
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<90 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<60 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<45 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<30 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<15 THEN 5
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<60 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<45 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<30 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<15 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<45 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<15 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<15 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb_eerste[2]))<30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krec_o_fb[2]))<15 THEN 1
        ELSE 0 END AS stijging_stadium_egfr_ckd,
        CAST(krec_o_fb_eerste[0] AS VARCHAR) AS krec_o_fb_eerste,
        TRY_TO_DATE(TO_VARCHAR(krec_o_fb_eerste[1])) AS krec_o_fb_eerste_dd,
        --mdrd
        CAST(round(CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))=0 THEN 0
            ELSE ((TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))-TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2])))/TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2])))*100
            END, 1) AS NUMBER(36, 1)) AS egfr_verloop_mdrd,
        CASE WHEN TRY_TO_DATE(TO_VARCHAR(krem_o_fb_eerste[1]))=TRY_TO_DATE(TO_VARCHAR(krem_o_fb[1])) THEN 0
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<90 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<60 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<45 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<30 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<15 THEN 5
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<60 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<45 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<30 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<15 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<45 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<15 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<15 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb_eerste[2]))<30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krem_o_fb[2]))<15 THEN 1
        ELSE 0 END AS stijging_stadium_egfr_mdrd,
        CAST(krem_o_fb_eerste[0] AS VARCHAR) AS krem_o_fb_eerste,
        TRY_TO_DATE(TO_VARCHAR(krem_o_fb_eerste[1])) AS krem_o_fb_eerste_dd,
        --creatinineklaring
        CAST(round(CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))=0 OR TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))=0 THEN 0
            ELSE ((TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))-TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2])))/TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2])))*100
            END, 1) AS NUMBER(36, 1)) AS egfr_verloop_kreat,
        CASE WHEN TRY_TO_DATE(TO_VARCHAR(krea_o_mk_eerste[1]))=TRY_TO_DATE(TO_VARCHAR(krea_o_mk[1])) THEN 0
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<90 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<60 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<45 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<30 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<15 THEN 5
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<60 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<45 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<30 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<90 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<15 THEN 4
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<4 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<60 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<15 THEN 3
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<45 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<15 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))>=15 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk_eerste[2]))<30 AND TRY_TO_DECFLOAT(TO_VARCHAR(krea_o_mk[2]))<15 THEN 1
        ELSE 0 END AS stijging_stadium_egfr_kreat,
        CAST(krea_o_mk_eerste[0] AS VARCHAR) AS krea_o_mk_eerste,
        TRY_TO_DATE(TO_VARCHAR(krea_o_mk_eerste[1])) AS krea_o_mk_eerste_dd,
        --albumine
        CAST(albk_u_mi_eerste[0] AS VARCHAR) AS albk_u_mi_eerste_meting,
        TRY_TO_DATE(TO_VARCHAR(albk_u_mi_eerste[1])) AS albk_u_mi_eerste_meting_datum,
        TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi_eerste[2])) AS albk_u_mi_eerste_meting_num,
        CASE WHEN TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi_eerste[2]))<3 AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>=3 AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))<=30 THEN 1
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi_eerste[2]))<3 AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi[2]))>30 THEN 2
            WHEN TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi_eerste[2]))>=3 AND TRY_TO_DECFLOAT(TO_VARCHAR(albk_u_mi_eerste[2]))<=30 THEN 1
        ELSE 0 END AS stijging_stadium_albuminurie,
        coalesce(egfr_daling_jaar,0) AS egfr_daling_jaar,
        coalesce(egfr_daling_jaar_creat_cyst,0) AS egfr_daling_jaar_creat_cyst,
        coalesce(egfr_daling_jaar_cyst,0) AS egfr_daling_jaar_cyst,
        coalesce(egfr_daling_jaar_ckd,0) AS egfr_daling_jaar_ckd,
        coalesce(egfr_daling_jaar_mdrd,0) AS egfr_daling_jaar_mdrd,
        coalesce(egfr_daling_jaar_kreat,0) AS egfr_daling_jaar_kreat,
        coalesce(overgevoelig_ace,0) AS overgevoelig_ace,
        --medicatie screening
        CASE WHEN cns_2018_medicatie_screening.patient_id IS NOT NULL THEN 1 ELSE 0 END AS medicatie_screening,
        medicatie_screening_atc,
        --medicatie oorzaak
        coalesce(metformine,0) AS metformine,
        coalesce(glibenclamide,0) AS glibenclamide,
        coalesce(glimepiride,0) AS glimepiride,
        coalesce(rosuvastatine,0) AS rosuvastatine,
        coalesce(glp1_agonisten,0) AS glp1_agonisten,
        coalesce(indomethacine,0) AS indomethacine,
        coalesce(ibuprofen,0) AS ibuprofen,
        coalesce(spironolacton,0) AS spironolacton,
        coalesce(co_trimoxazol,0) AS co_trimoxazol,
        coalesce(aminoglycosiden,0) AS aminoglycosiden,
        coalesce(nitrofurantoine,0) AS nitrofurantoine,
        coalesce(nsaids,0) AS nsaids,
        coalesce(diclofenac,0) AS diclofenac,
        coalesce(naproxen,0) AS naproxen,
        coalesce(colchicine,0) AS colchicine,
        coalesce(alendroninezuur,0) AS alendroninezuur,
        coalesce(lithium,0) AS lithium,
        coalesce(cetirizine,0) AS cetirizine,
        --missende icpc
        CASE WHEN cns_2018_albuminurie.patient_id IS NOT NULL THEN 1 ELSE 0 END AS albuminurie_3metingen,
        CASE WHEN cns_2018_albuminurie_matig.patient_id IS NOT NULL THEN 1 ELSE 0 END AS albuminurie_matig_3metingen,
        CASE WHEN cns_2018_albuminurie_ernstig.patient_id IS NOT NULL THEN 1 ELSE 0 END AS albuminurie_ernstig_3metingen,
        CASE WHEN cns_2018_verlaagd_egfr.patient_id IS NOT NULL THEN 1 ELSE 0 END AS verlaagd_egfr_3metingen,
        CASE WHEN cns_2018_normaal_egfr.patient_id IS NOT NULL THEN 1 ELSE 0 END AS normaal_egfr_3metingen,
        --allergie
        COALESCE(allergie_ace_remmers,0) AS allergie_ace_remmers,
        COALESCE(allergie_angiotensine2antagonisten,0) AS allergie_angiotensine2antagonisten,
        --rrsy (alle)
        TRY_TO_DECFLOAT(TO_VARCHAR(ltste_rrsy_rrsy_ka[0])) AS ltste_rrsy_rrsy_ka_num,
        TRY_TO_DECFLOAT(TO_VARCHAR(ltste_rrsy_rrgs_ka_mh[0])) AS ltste_rrsy_rrgs_ka_mh_num,
        TRY_TO_DECFLOAT(TO_VARCHAR(ltste_rrsy_rrs7_ka_mh[0])) AS ltste_rrsy_rrs7_ka_mh_num,
        TRY_TO_DECFLOAT(TO_VARCHAR(ltste_rrsy_rr3s_ka[0])) AS ltste_rrsy_rr3s_ka_num,
        --rrsy_ka
        COALESCE(onder_strf_130_rrsy_ka,0) AS onder_strf_130_rrsy_ka,
        COALESCE(onder_strf_140_rrsy_ka,0) AS onder_strf_140_rrsy_ka,
        COALESCE(onder_strf_150_rrsy_ka,0) AS onder_strf_150_rrsy_ka,
        ltste_meting_rrsy_ka_num,
        vrltste_meting_rrsy_ka_num,
        --rrsy_gem
        COALESCE(onder_strf_120_rrgs_ka_mh,0) AS onder_strf_120_rrgs_ka_mh,
        COALESCE(onder_strf_130_rrgs_ka_mh,0) AS onder_strf_130_rrgs_ka_mh,
        COALESCE(onder_strf_140_rrgs_ka_mh,0) AS onder_strf_140_rrgs_ka_mh,
        COALESCE(onder_strf_125_rrs7_ka_mh,0) AS onder_strf_125_rrs7_ka_mh,
        COALESCE(onder_strf_125_rr3s_ka,0) AS onder_strf_125_rr3s_ka,
        COALESCE(onder_strf_135_rrs7_ka_mh,0) AS onder_strf_135_rrs7_ka_mh,
        COALESCE(onder_strf_135_rr3s_ka,0) AS onder_strf_135_rr3s_ka,
        COALESCE(onder_strf_145_rrs7_ka_mh,0) AS onder_strf_145_rrs7_ka_mh,
        COALESCE(onder_strf_145_rr3s_ka,0) AS onder_strf_145_rr3s_ka,
        ltste_meting_rrgs_ka_mh_num,
        ltste_meting_rrs7_ka_mh_num,
        ltste_meting_rr3s_ka_num,
        vrltste_meting_rrgs_ka_mh_num,
        vrltste_meting_rrs7_ka_mh_num,
        vrltste_meting_rr3s_ka_num,
        CASE WHEN ltste_meting_rrsy_ka_num IS NOT NULL THEN vrltste_meting_rrsy_ka_num
             WHEN ltste_meting_rrgs_ka_mh_num IS NOT NULL THEN vrltste_meting_rrgs_ka_mh_num
             WHEN ltste_meting_rrs7_ka_mh_num IS NOT NULL THEN vrltste_meting_rrs7_ka_mh_num
             WHEN ltste_meting_rr3s_ka_num IS NOT NULL THEN vrltste_meting_rr3s_ka_num
            ELSE NULL END AS vrltste_meting_rrsy,
        CASE WHEN ltste_meting_rrsy_ka_num IS NOT NULL THEN vrltste_meting_rrsy_ka_datum
             WHEN ltste_meting_rrgs_ka_mh_num IS NOT NULL THEN vrltste_meting_rrgs_ka_mh_datum
             WHEN ltste_meting_rrs7_ka_mh_num IS NOT NULL THEN vrltste_meting_rrs7_ka_mh_datum
             WHEN ltste_meting_rr3s_ka_num IS NOT NULL THEN vrltste_meting_rr3s_ka_datum
            ELSE NULL END AS vrltste_meting_rrsy_datum
    FROM proigia_patienten_gegevens
    LEFT JOIN proigia_episode_seg2 USING (patient_id)
    LEFT JOIN proigia_meetwaarden_seg2 USING (patient_id)
    LEFT JOIN proigia_dubbele_hb_seg2 USING (patient_id)
    LEFT JOIN proigia_medicatie_seg2 USING (patient_id)
    LEFT JOIN proigia_mw_rh_seg2 USING (patient_id)
    LEFT JOIN proigia_mw_rh_laatste_seg2 USING (patient_id)
    LEFT JOIN proigia_allergie USING (patient_id)
    LEFT JOIN cns_2018_meetwaarden_egfr USING (patient_id)
    LEFT JOIN cns_2018_meetwaarden_egfr_eerste USING (patient_id)
    LEFT JOIN cns_2018_meetwaarden_albk_u_mi_eerste USING (patient_id)
    LEFT JOIN cns_2018_meetwaarden_egfr_daling_jaar USING (patient_id)
    LEFT JOIN cns_2018_ci USING (patient_id)
    LEFT JOIN cns_2018_apotheek a USING (patient_id)
    LEFT JOIN cns_2018_verzameltabel_3mt USING (patient_id)
    LEFT JOIN cns_2018_overgevoelig_ace USING (patient_id)
    LEFT JOIN cns_2018_medicatie_screening USING (patient_id)
    LEFT JOIN cns_2018_medicatie_oorzaak USING (patient_id)
    LEFT JOIN cns_2018_albuminurie USING (patient_id)
    LEFT JOIN cns_2018_albuminurie_matig USING (patient_id)
    LEFT JOIN cns_2018_albuminurie_ernstig USING (patient_id)
    LEFT JOIN cns_2018_verlaagd_egfr USING (patient_id)
    LEFT JOIN cns_2018_normaal_egfr USING (patient_id)
    LEFT JOIN cns_2018_rrsy_alle USING (patient_id)
    LEFT JOIN cns_2018_rrsy_ka USING (patient_id)
    LEFT JOIN cns_2018_rrsy_gem USING (patient_id)
    LEFT JOIN cns_2018_ldl USING (patient_id)
    LEFT JOIN cns USING (patient_id)
    ), 
    pre_doelgroepen AS
    (
    SELECT
        *,
        CASE WHEN vvr=1 AND hvz=0 AND l88=0 AND c03_voorschriften+c09_voorschriften>0 AND c10_voorschriften=0 AND laatste_ldl_gereguleerd=1 THEN 1 ELSE 0 END AS cvrm_2,
        CASE WHEN vvr=1 AND hvz=0 AND l88=0 AND c03_voorschriften+c09_voorschriften>0 AND c10_voorschriften>0 THEN 1 ELSE 0 END AS cvrm_3,
        CASE WHEN vvr=1 AND hvz=0 AND l88=0 AND laatste_ldl_gereguleerd=0 AND ((laatste_rrsy_num>140 AND leeftijd<80) OR (laatste_rrsy_num>160 AND leeftijd>=80)) THEN 1 ELSE 0 END AS cvrm_5,
        CASE WHEN m04aa01_voorschriften>0 THEN 1 ELSE 0 END AS allopurinol,
        CASE WHEN c01aa_voorschriften>0 THEN 1 ELSE 0 END AS lanoxin,
        CASE WHEN (b01ae_voorschriften+b01af_voorschriften>0) AND laatste_egfr_num_std>=60 THEN 1 ELSE 0 END AS noac_egfr60p,
    --kwetsbaarheid
        CASE WHEN awkw_az='ja'
                OR kwbh_az='ja'
                OR kwcz_az='ja'
                OR a05=1
        THEN 1 ELSE 0 END AS kwetsbaarheid,
    --Risicocat.
        CASE WHEN cns_geen_std=1 THEN 'geen CNS'
             WHEN cns_mild_verhoogd_risico_std=1 THEN 'mild verhoogd'
             WHEN cns_matig_verhoogd_risico_std=1 THEN 'matig verhoogd'
             WHEN cns_sterk_verhoogd_risico_std=1 THEN 'sterk verhoogd'
            ELSE NULL END AS cns_std,
        CASE WHEN cns_geen_lab=1 THEN 'geen CNS'
             WHEN cns_mild_verhoogd_risico_lab=1 THEN 'mild verhoogd'
             WHEN cns_matig_verhoogd_risico_lab=1 THEN 'matig verhoogd'
             WHEN cns_sterk_verhoogd_risico_lab=1 THEN 'sterk verhoogd'
            ELSE NULL END AS cns_lab
    FROM pre_verzamel
    )
    SELECT
    *,
    --redenen screening
        CASE WHEN leeftijd>=70
                    AND (cns_mild_verhoogd_risico_std=1
                        OR cns_matig_verhoogd_risico_std=1
                        OR cns_sterk_verhoogd_risico_std=1)
                    AND medicatie_screening=1 
                    AND (dmhb_tz!='specialist' OR dmhb_tz IS NULL) 
                    AND (cvhb_kz!='specialist' OR cvhb_kz IS NULL)
                        THEN 1 ELSE 0 END AS screening_leeftijd_medicatie,
        CASE WHEN icpc_urinewegen_tbv_screening=1
                    AND (dmhb_tz!='specialist' OR dmhb_tz IS NULL) 
                    AND (cvhb_kz!='specialist' OR cvhb_kz IS NULL)
            THEN 1 ELSE 0 END AS screening_icpc_urinewegen,
        CASE WHEN laatste_egfr_num_std<60
                    AND (dmhb_tz!='specialist' OR dmhb_tz IS NULL) 
                    AND (cvhb_kz!='specialist' OR cvhb_kz IS NULL)
            THEN 1 ELSE 0 END AS screening_egfr,
        CASE WHEN (cvrm_2=1 OR cvrm_3=1 OR cvrm_5=1 OR hvz=1 OR l88=1 OR dm2=1 OR allopurinol=1 OR lanoxin=1 OR noac_egfr60p=1)
                    AND (dmhb_tz!='specialist' OR dmhb_tz IS NULL) 
                    AND (cvhb_kz!='specialist' OR cvhb_kz IS NULL)
            THEN 1 ELSE 0 END AS screening_andere_reden,
    --stadiering
        CASE WHEN laatste_egfr_num_std>=90 THEN 'G1'
             WHEN laatste_egfr_num_std>=60 THEN 'G2'
             WHEN laatste_egfr_num_std>=45 THEN 'G3a'
             WHEN laatste_egfr_num_std>=30 THEN 'G3b'
             WHEN laatste_egfr_num_std>=15 THEN 'G4'
             WHEN laatste_egfr_num_std>=0 THEN 'G5'
            ELSE NULL END AS stadium_g,
        CASE WHEN albk_u_mi_num>30 THEN 'A3'
             WHEN albk_u_mi_num>=3 THEN 'A2'
             WHEN albk_u_mi_num>=0 THEN 'A1'
            ELSE NULL END AS stadium_a,
    --stadiering_3mt
        CASE WHEN laatste_egfr_num_3mt>=90 THEN 'G1'
             WHEN laatste_egfr_num_3mt>=60 THEN 'G2'
             WHEN laatste_egfr_num_3mt>=45 THEN 'G3a'
             WHEN laatste_egfr_num_3mt>=30 THEN 'G3b'
             WHEN laatste_egfr_num_3mt>=15 THEN 'G4'
             WHEN laatste_egfr_num_3mt>=0 THEN 'G5'
            ELSE NULL END AS stadium_g_3mt,
        CASE WHEN albk_u_mi_num_3mt>30 THEN 'A3'
             WHEN albk_u_mi_num_3mt>=3 THEN 'A2'
             WHEN albk_u_mi_num_3mt>=0 THEN 'A1'
            ELSE NULL END AS stadium_a_3mt,
        CASE WHEN ((egfr_verloop_kreat<=-25
                AND (stijging_stadium_albuminurie+stijging_stadium_egfr_kreat)>=1)
            OR (egfr_verloop_mdrd<=-25
                AND (stijging_stadium_albuminurie+stijging_stadium_egfr_mdrd)>=1)
            OR (egfr_verloop_ckd<=-25
                AND (stijging_stadium_albuminurie+stijging_stadium_egfr_ckd)>=1)
            OR (egfr_verloop_cyst<=-25
                AND (stijging_stadium_albuminurie+stijging_stadium_egfr_cyst)>=1)
            OR (egfr_verloop_creat_cyst<=-25
                AND (stijging_stadium_albuminurie+stijging_stadium_egfr_creat_cyst)>=1)
            OR egfr_daling_jaar=1)
            AND laatste_egfr_num_std<60
            THEN 1 ELSE 0 END AS progressie_nierfunctieverlies,
        CASE WHEN (laatste_rrsy_num>=140
                AND DATEADD(YEAR, -1, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))<=laatste_rrsy_laatste_dd
                OR k86=1 OR k87=1)
            AND overgevoelig_ace=0
            THEN 1 ELSE 0 END AS indicatie_ace_remmer,			
    --bloeddruk verhoogd (kopie van cvrm_2019)
        CASE WHEN leeftijd<=70
                AND ((ltste_rrsy_rrsy_ka_num>130 AND vrltste_meting_rrsy_ka_num>130)
                    OR (ltste_rrsy_rrgs_ka_mh_num>120 AND vrltste_meting_rrgs_ka_mh_num>120)
                    OR (ltste_rrsy_rrs7_ka_mh_num>125 AND vrltste_meting_rrs7_ka_mh_num>125)
                    OR (ltste_rrsy_rr3s_ka_num>125 AND vrltste_meting_rr3s_ka_num>125)) THEN 1 
            WHEN leeftijd>70
                AND ((ltste_rrsy_rrsy_ka_num>140 AND vrltste_meting_rrsy_ka_num>140)
                    OR (ltste_rrsy_rrgs_ka_mh_num>130 AND vrltste_meting_rrgs_ka_mh_num>130)
                    OR (ltste_rrsy_rrs7_ka_mh_num>135 AND vrltste_meting_rrs7_ka_mh_num>135)
                    OR (ltste_rrsy_rr3s_ka_num>135 AND vrltste_meting_rr3s_ka_num>135)) THEN 1
            ELSE 0 END AS doelgroep_rrsyst_boven_strf,
        CASE WHEN leeftijd<=70
                AND (
                    rrsw_kq_num>129
                    OR onder_strf_130_rrsy_ka=1
                    OR onder_strf_120_rrgs_ka_mh=1
                    OR onder_strf_125_rrs7_ka_mh=1
                    OR onder_strf_125_rr3s_ka=1
                    OR (kwetsbaarheid=1
                        AND (onder_strf_150_rrsy_ka=1
                            OR onder_strf_140_rrgs_ka_mh=1
                            OR onder_strf_145_rrs7_ka_mh=1
                            OR onder_strf_145_rr3s_ka=1
                        ))
                    OR laatste_rrdi_num<70
                    OR GREATEST_IGNORE_NULLS(rrgs_ka_mh_laatste_dd,rrs7_ka_mh_laatste_dd,rr3s_ka_laatste_dd,rrsy_ka_laatste_dd) <DATEADD(MONTH, -13, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))
                    )	THEN 1
            WHEN leeftijd>70
                AND (
                    rrsw_kq_num>139
                    OR onder_strf_140_rrsy_ka=1
                    OR onder_strf_130_rrgs_ka_mh=1
                    OR onder_strf_135_rrs7_ka_mh=1
                    OR onder_strf_135_rr3s_ka=1
                    OR (kwetsbaarheid=1
                        AND (onder_strf_150_rrsy_ka=1
                            OR onder_strf_140_rrgs_ka_mh=1
                            OR onder_strf_145_rrs7_ka_mh=1
                            OR onder_strf_145_rr3s_ka=1
                        ))
                    OR laatste_rrdi_num<70
                    OR GREATEST_IGNORE_NULLS(rrgs_ka_mh_laatste_dd,rrs7_ka_mh_laatste_dd,rr3s_ka_laatste_dd,rrsy_ka_laatste_dd) <DATEADD(MONTH, -13, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))
                    )	THEN 1 
                ELSE 0 END AS doelgroep_exclusie_strf_bloeddruk,
        --een deel is uitgecommentarieerd omdat deze regels niet voor dit rapport relevant zijn; 
        --voor cvrm_2019 daarentegen wel, omdat je daarbij pt met diast. <70 niet wil behandelen met hypertensiva
        --en je patienten uit je lijst wilt kunnen schrappen met meetwaarden rrsw_kq         
        CASE WHEN leeftijd<=70
                AND (
                    --rrsw_kq_num>129
                    onder_strf_130_rrsy_ka=1
                    OR onder_strf_120_rrgs_ka_mh=1
                    OR onder_strf_125_rrs7_ka_mh=1
                    OR onder_strf_125_rr3s_ka=1
                    OR (kwetsbaarheid=1
                        AND (onder_strf_150_rrsy_ka=1
                            OR onder_strf_140_rrgs_ka_mh=1
                            OR onder_strf_145_rrs7_ka_mh=1
                            OR onder_strf_145_rr3s_ka=1
                        ))
                    --OR laatste_rrdi_num<70
                    OR GREATEST_IGNORE_NULLS(rrgs_ka_mh_laatste_dd,rrs7_ka_mh_laatste_dd,rr3s_ka_laatste_dd,rrsy_ka_laatste_dd) <DATEADD(MONTH, -13, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))
                    )	THEN 1
            WHEN leeftijd>70
                AND (
                    --rrsw_kq_num>139
                    onder_strf_140_rrsy_ka=1
                    OR onder_strf_130_rrgs_ka_mh=1
                    OR onder_strf_135_rrs7_ka_mh=1
                    OR onder_strf_135_rr3s_ka=1
                    OR (kwetsbaarheid=1
                        AND (onder_strf_150_rrsy_ka=1
                            OR onder_strf_140_rrgs_ka_mh=1
                            OR onder_strf_145_rrs7_ka_mh=1
                            OR onder_strf_145_rr3s_ka=1
                        ))
                    --OR laatste_rrdi_num<70
                    OR GREATEST_IGNORE_NULLS(rrgs_ka_mh_laatste_dd,rrs7_ka_mh_laatste_dd,rr3s_ka_laatste_dd,rrsy_ka_laatste_dd) <DATEADD(MONTH, -13, TRY_TO_DATE(TO_VARCHAR('${peildatum}')))
                    )	THEN 1 
                ELSE 0 END AS doelgroep_exclusie_strf_bloeddruk_tbv_oorzaak_cns,
        CASE WHEN leeftijd>70
                AND hvz=0
                AND kwetsbaarheid=1
            THEN 1 ELSE 0 END AS doelgroep_kwetsbaar_zonder_hvz                
    FROM pre_doelgroepen
    WHERE leeftijd>=18;