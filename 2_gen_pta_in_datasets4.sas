/************************************************************************************************************
************************************************************************************************************
Program: gen_pta_in_datasets4
Created By: Brendan Rabideau
Created Date: 5/15/15
Updated Date: 6/7/16
Purpose: The purpose of this program is to to generate the raw datasets used in Yale's Analytic Files, 
  		 which create datasets to be used in Yale's 30day Unplanned Readmission Algorithim. The program creates
  		 the pta_in_base and pta_in_line datasets.

Notes: 
Updates:
	6/12/15 - Added years going back to 2002, updated to match Yale's inclusion criteria
    6/24/15 - Updated 2002-2005 to use EHIC-bene_id xwalked datasets
			- Stopped collapsing data down to the stay-level since Yale's code already does this (macro'd out code, %macro collapse)
			- Removed MedPAR merge and created desired variables according to Resdac specifications instead
	6/25/15 - clm_id (becomes hse_unique_id and merges line and base) likely claimindex pre-2005. Reformatted to match clm_id
	7/06/15 - Dropped obs with admissions before the 1st year of our data (2002) 
	7/29/15 - Updated to include IRF stays as well as SNF stays in the post acute care stay file
	11/24/15- Converting for the HRRP project
	11/24/15- Making the process of reading in raw data more automated - macro-ing libnames, making start and end year set statements self-generating
	12/09/15- Added HHA and Outpatient to the pta_base_dataset_pac dataset
	06/07/16- Expanded going back to 2003 using the RAND 100% sample. There are separate files for acute care patients and SNF/IRF/LTC patients
			  due to the way RAND gave us data. Patients pre-2006 use BID instead of bene_id. Use bid_bene xwalk.
************************************************************************************************************
************************************************************************************************************/

options compress=yes mprint;

%let out = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Raw;
libname out "&out.";
libname mds "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MDS";

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";
%let syr=20&YY.;
%let eyr=20&YYE.;

/*********************************************************************************************
*********************************************************************************************/
/*If this program has been run multiple times, delete its output dataset (if it exists)
  so that if an issue occurs we don't accidentally use a previous iteration of the dataset
  and are forced to correct the issue*/
%macro remove(ds);
	%if %sysfunc(exist(&ds.)) = 1 %then %do;
		proc delete data=&ds.;
	%end;
%mend;
%remove(ds=out.pta_in_base_dataset);
%remove(ds=out.pta_in_base_dataset_pac);
/**********************************************************************************************
**********************************************************************************************/

/************************************************************************************
PART A INPATIENT BASE DATASET (IP Claims)
************************************************************************************/

/*Use raw MedPAR files - standardize names as necessary - definitely different pre-2010*/
%macro rename_ds(start_yr, end_yr);
	%do i = &start_yr. %to &end_yr.;

		/*Different years require different renaming. Early years require special xwalked datasets that map EHIC to
		  bene_id, which gets used from 2005 onwards.*/

	    /*2002-2005*/
		%if 2002<=&i. & &i.<=2005 %then %do;
			libname xw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/b2006_2008/Request863";
			libname med&i. "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/cms7707/DATA";

			proc contents data=med&i..acute&i.; run;

			data med&i. (rename=(BID_=BID));
				set med&i..acute&i.(where=(BID_~=""));
				rename diag1-diag10=dgnscd1-dgnscd10
					   surg1-surg6=prcdrcd1-prcdrcd6
					   provno=prvdrnum
					   totchrg=tot_chg
					   medpay=pmt_amt
					   provcode=spclunit;

				admsndt=admdte_;
				dschrgdt=disdte_;
				format admsndt dschrgdt DATE9.;

				/*Standardize vartype for important vars; drop problematic, superfluous vars*/
 				c_drg_cd=put(input(drg,8.),z3.);
				c_DSTNTNCD=put(distdest,2.);
				drop drg distdest;

				rename c_drg_cd=DRG_CD
					   c_DSTNTNCD=DSTNTNCD;
			run;

			/*Merge on the BID --> Bene_ID crosswalk*/
			data xwalk;
				set xw.bid_bene_xwalk (where=(BID~=""));
			run;

			proc sort data=med&i.; by BID; run;
			proc sort data=xwalk nodupkey; by BID; run;

			data med&i. (drop=BID rename=(hicno=bene_id)); /*We will rename bene_id to hicno again later, but for not keep it consistent with other years*/
				merge med&i. (in=a)
					  xwalk (in=b);
				by BID;
				if a & b;
			run;

			proc contents data=xw.bid_bene_xwalk; run;
			proc contents data=med2003; run;
			proc print data=med2003 (obs=5); run;
		%end;

		/*2006-2009*/
		%if 2006<=&i. & &i.<=2009 %then %do;
			libname med&i. "&med.";

			data med&i.;
				set med&i..medpar&i. (where=(bene_id~=""));

				/*Standardize vartype for important vars; drop problematic, superfluous vars*/
 				c_drg_cd=put(input(DRG_CD,8.),z3.);
				c_DSTNTNCD=put(DSTNTNCD,2.);
				drop  ADMSNDAY PHRMCYCD TRNSPLNT ONCLGYSW DGNSTCSW THRPTCSW NUCLR_SW CTSCANSW 
					  IMGNG_SW OPSRVCCD DRG_CD DSTNTNCD OUTLR_CD ESRD_CD IPSBCD FILDTCD SMPLSIZE WRNGCD;
				rename c_drg_cd=DRG_CD
					   c_DSTNTNCD=DSTNTNCD
					   TOTCHRG=tot_chg;
			run;
		%end;


		/*2010+*/
		%if 2010<=&i. %then %do;
			libname med&i. "&med.";

			data med&i.;
				set med&i..medpar&i. (where=(bene_id~=""));
				/*Renaming variables that have the # in the middle. Only occurs post-2010*/
				array poa_dgns {25} $ poa_dgns_1_ind_cd  poa_dgns_2_ind_cd  poa_dgns_3_ind_cd  poa_dgns_4_ind_cd  poa_dgns_5_ind_cd
									  poa_dgns_6_ind_cd  poa_dgns_7_ind_cd  poa_dgns_8_ind_cd  poa_dgns_9_ind_cd  poa_dgns_10_ind_cd
	                                  poa_dgns_11_ind_cd poa_dgns_12_ind_cd poa_dgns_13_ind_cd poa_dgns_14_ind_cd poa_dgns_15_ind_cd
	                                  poa_dgns_16_ind_cd poa_dgns_17_ind_cd poa_dgns_18_ind_cd poa_dgns_19_ind_cd poa_dgns_20_ind_cd
									  poa_dgns_21_ind_cd poa_dgns_22_ind_cd poa_dgns_23_ind_cd poa_dgns_24_ind_cd poa_dgns_25_ind_cd;

				array poa_dgns_e {12} $ poa_dgns_e_1_ind_cd  poa_dgns_e_2_ind_cd  poa_dgns_e_3_ind_cd  poa_dgns_e_4_ind_cd
								        poa_dgns_e_5_ind_cd  poa_dgns_e_6_ind_cd  poa_dgns_e_7_ind_cd  poa_dgns_e_8_ind_cd
								        poa_dgns_e_9_ind_cd  poa_dgns_e_10_ind_cd poa_dgns_e_11_ind_cd poa_dgns_e_12_ind_cd;

				array dgns_e {12}	  $ dgns_e_1_cd dgns_e_2_cd dgns_e_3_cd dgns_e_4_cd  dgns_e_5_cd  dgns_e_6_cd
										dgns_e_7_cd dgns_e_8_cd dgns_e_9_cd dgns_e_10_cd dgns_e_11_cd dgns_e_12_cd;

				array POANCD {25}     $ POANCD1-POANCD25;
				array POAEND {12} 	  $ POAEND01-POAEND12;
				array EDGSCD {12}     $ EDGSCD01-EDGSCD12;

				do j=1 to 25;
					POANCD{j} = poa_dgns{j};
					if j <= 12 then do;
						POAEND{j} = poa_dgns_e{j};
						EDGSCD{j} = dgns_e{j};
					end;
				end;

				/*Standardize vartype for important vars; drop problematic, superfluous vars*/
 				c_drg_cd=put(input(DRG_CD,8.),z3.);
				c_DSTNTNCD=put(DSTNTNCD,2.);
				drop  ADMSNDAY PHRMCYCD TRNSPLNT ONCLGYSW DGNSTCSW THRPTCSW NUCLR_SW CTSCANSW 
					  IMGNG_SW OPSRVCCD DRG_CD DSTNTNCD OUTLR_CD ESRD_CD IPSBCD FILDTCD SMPLSIZE WRNGCD;
				rename c_drg_cd=DRG_CD
					   c_DSTNTNCD=DSTNTNCD
					   TOTCHRG=tot_chg;
			run;
		%end;
	%end;
%mend;
%rename_ds(&syr.,&eyr.);

data med (keep= dgnscd1-dgnscd25
				dgns_vrsn_cd_1-dgns_vrsn_cd_25
				poancd1-poancd25
				poaend01-poaend12
				edgscd01-edgscd12
				dgns_e_vrsn_cd_1-dgns_e_vrsn_cd_12
				prcdrcd1-prcdrcd25
				srgcl_prcdr_vrsn_cd_1-srgcl_prcdr_vrsn_cd_25
				prcdrdt1-prcdrdt25
			    /*Other Vars*/
			    bene_id medparid npi_at upin_at npi_op upin_op clm_type mdcl_rec spclunit
				prvdrnum state_cd type_adm dschrgdt admsndt src_adms dstntncd dschrgcd drg_cd util_day los
				icuindcd crnry_cd sslssnf qlfyfrom qlfythru admsour ms_cd pmt_amt tot_chg year_disch year_adm);

	set	med&syr.-med&eyr.;	
		
		/*Vars we don't have and aren't important, but are required to exist to make the proceding programs run*/
		NPI_AT="";
		UPIN_AT="";
		NPI_OP="";
		UPIN_OP="";
		MDCL_REC="";
		ADMSOUR = "";

		if icuindcd ~in("","6") then icuind=1;
		if crnry_cd~="" then ccuind=1;

		year_disch=year(dschrgdt);
		year_adm=year(admsndt);
run;

proc freq data=med;
	tables sslssnf clm_type year_disch year_adm;
run;

proc sort data=med; by bene_id admsndt dschrgdt; run;

/*Subset and clean up the dataset to make it look like Yale's pta_in_base_dataset*/
data out.pta_in_base_dataset (keep = ADMIT ADMSOUR BENE_CLM_NUM CASE_TYPE CLM_ADMSN_DT DDEST DIAG1 DIAG2 DIAG3 
									 DIAG4 DIAG5 DIAG6 DIAG7 DIAG8 DIAG9 DIAG10 DIAG11 DIAG12 DIAG13 DIAG14 DIAG15 
									 DIAG16 DIAG17 DIAG18 DIAG19 DIAG20 DIAG21 DIAG22 DIAG23 DIAG24 DIAG25 DISCH 
									 DISST DRGCD DVRSND01 DVRSND02 DVRSND03 DVRSND04 DVRSND05 DVRSND06 DVRSND07 DVRSND08 
									 DVRSND09 DVRSND10 DVRSND11 DVRSND12 DVRSND13 DVRSND14 DVRSND15 DVRSND16 DVRSND17 
									 DVRSND18 DVRSND19 DVRSND20 DVRSND21 DVRSND22 DVRSND23 DVRSND24 DVRSND25 EDGSCD01 
									 EDGSCD02 EDGSCD03 EDGSCD04 EDGSCD05 EDGSCD06 EDGSCD07 EDGSCD08 EDGSCD09 EDGSCD10 
									 EDGSCD11 EDGSCD12 EVRSCD01 EVRSCD02 EVRSCD03 EVRSCD04 EVRSCD05 EVRSCD06 EVRSCD07 
									 EVRSCD08 EVRSCD09 EVRSCD10 EVRSCD11 EVRSCD12 HICNO HSE_UNIQUE_ID MDCL_REC MSCD 
									 NCH_CLM_TYPE_CD NPI_AT NPI_OP POAEND01 POAEND02 POAEND03 POAEND04 POAEND05 POAEND06 
									 POAEND07 POAEND08 POAEND09 POAEND10 POAEND11 POAEND12 POANCD1 POANCD2 POANCD3 POANCD4 
									 POANCD5 POANCD6 POANCD7 POANCD8 POANCD9 POANCD10 POANCD11 POANCD12 POANCD13 POANCD14 POANCD15 
									 POANCD16 POANCD17 POANCD18 POANCD19 POANCD20 POANCD21 POANCD22 POANCD23 POANCD24 POANCD25 
									 PROC1 PROC2 PROC3 PROC4 PROC5 PROC6 PROC7 PROC8 PROC9 PROC10 PROC11 PROC12 PROC13 PROC14 
									 PROC15 PROC16 PROC17 PROC18 PROC19 PROC20 PROC21 PROC22 PROC23 PROC24 PROC25 PROCDT1 PROCDT2
									 PROCDT3 PROCDT4 PROCDT5 PROCDT6 PROCDT7 PROCDT8 PROCDT9 PROCDT10 PROCDT11 PROCDT12 PROCDT13 
    								 PROCDT14 PROCDT15 PROCDT16 PROCDT17 PROCDT18 PROCDT19 PROCDT20 PROCDT21 PROCDT22 PROCDT23 
									 PROCDT24 PROCDT25 PROVID PSTATE_ALPHA PVSNCD01 PVSNCD02 PVSNCD03 PVSNCD04 PVSNCD05 
									 PVSNCD06 PVSNCD07 PVSNCD08 PVSNCD09 PVSNCD10 PVSNCD11 PVSNCD12 PVSNCD13 PVSNCD14 PVSNCD15 
									 PVSNCD16 PVSNCD17 PVSNCD18 PVSNCD19 PVSNCD20 PVSNCD21 PVSNCD22 PVSNCD23 PVSNCD24 PVSNCD25 
									 TYPEADM UPIN_AT UPIN_OP factype sslssnf pmt_amt tot_chg spclunit util_day los)
	out.pta_in_base_dataset_pac (keep= ADMIT ADMSOUR BENE_CLM_NUM CASE_TYPE CLM_ADMSN_DT DDEST DIAG1 DIAG2 DIAG3 
									 DIAG4 DIAG5 DIAG6 DIAG7 DIAG8 DIAG9 DIAG10 DIAG11 DIAG12 DIAG13 DIAG14 DIAG15 
									 DIAG16 DIAG17 DIAG18 DIAG19 DIAG20 DIAG21 DIAG22 DIAG23 DIAG24 DIAG25 DISCH 
									 DISST DRGCD DVRSND01 DVRSND02 DVRSND03 DVRSND04 DVRSND05 DVRSND06 DVRSND07 DVRSND08 
									 DVRSND09 DVRSND10 DVRSND11 DVRSND12 DVRSND13 DVRSND14 DVRSND15 DVRSND16 DVRSND17 
									 DVRSND18 DVRSND19 DVRSND20 DVRSND21 DVRSND22 DVRSND23 DVRSND24 DVRSND25 EDGSCD01 
									 EDGSCD02 EDGSCD03 EDGSCD04 EDGSCD05 EDGSCD06 EDGSCD07 EDGSCD08 EDGSCD09 EDGSCD10 
									 EDGSCD11 EDGSCD12 EVRSCD01 EVRSCD02 EVRSCD03 EVRSCD04 EVRSCD05 EVRSCD06 EVRSCD07 
									 EVRSCD08 EVRSCD09 EVRSCD10 EVRSCD11 EVRSCD12 HICNO HSE_UNIQUE_ID MDCL_REC MSCD 
									 NCH_CLM_TYPE_CD NPI_AT NPI_OP POAEND01 POAEND02 POAEND03 POAEND04 POAEND05 POAEND06 
									 POAEND07 POAEND08 POAEND09 POAEND10 POAEND11 POAEND12 POANCD1 POANCD2 POANCD3 POANCD4 
									 POANCD5 POANCD6 POANCD7 POANCD8 POANCD9 POANCD10 POANCD11 POANCD12 POANCD13 POANCD14 POANCD15 
									 POANCD16 POANCD17 POANCD18 POANCD19 POANCD20 POANCD21 POANCD22 POANCD23 POANCD24 POANCD25 
									 PROC1 PROC2 PROC3 PROC4 PROC5 PROC6 PROC7 PROC8 PROC9 PROC10 PROC11 PROC12 PROC13 PROC14 
									 PROC15 PROC16 PROC17 PROC18 PROC19 PROC20 PROC21 PROC22 PROC23 PROC24 PROC25 PROCDT1 PROCDT2
									 PROCDT3 PROCDT4 PROCDT5 PROCDT6 PROCDT7 PROCDT8 PROCDT9 PROCDT10 PROCDT11 PROCDT12 PROCDT13 
    								 PROCDT14 PROCDT15 PROCDT16 PROCDT17 PROCDT18 PROCDT19 PROCDT20 PROCDT21 PROCDT22 PROCDT23 
									 PROCDT24 PROCDT25 PROVID PSTATE_ALPHA PVSNCD01 PVSNCD02 PVSNCD03 PVSNCD04 PVSNCD05 
									 PVSNCD06 PVSNCD07 PVSNCD08 PVSNCD09 PVSNCD10 PVSNCD11 PVSNCD12 PVSNCD13 PVSNCD14 PVSNCD15 
									 PVSNCD16 PVSNCD17 PVSNCD18 PVSNCD19 PVSNCD20 PVSNCD21 PVSNCD22 PVSNCD23 PVSNCD24 PVSNCD25 
									 TYPEADM UPIN_AT UPIN_OP sslssnf factype pmt_amt tot_chg spclunit util_day los);
	set med (where=(bene_id ~= "" & admsndt > MDY(01,01,20&YY.))); /*No admissions before the 1st date of our data*/

	label
		bene_id  = "Encrypted 723 Beneficiary ID"
		medparid = "Unique Key for CCW MedPAR Table"
		prvdrnum = "MEDPAR Provider Number"
		admsndt	 = "Date beneficiary admitted for Inpatient care or date care started"
		type_adm = "Type and priority of benes admission to facility for Inp hosp stay code"
		dschrgdt = "Date beneficiary was discharged or died"
		dschrgcd = "Code identifying status of patient as of CLM_THRU_DT"
		dstntncd = "Destination upon discharge from facility code"
		clm_type = "NCH Claim Type Code"
		crnry_cd = "Coronary care unit type code"
		icuindcd = "ICU type code";

	/*Rename variables to match the Yale programs*/
	rename dgnscd1-dgnscd25               				  = DIAG1-DIAG25
		   dgns_vrsn_cd_1-dgns_vrsn_cd_25     			  = DVRSND01-DVRSND25
		   prcdrcd1-prcdrcd25             				  = PROC1-PROC25
		   srgcl_prcdr_vrsn_cd_1-srgcl_prcdr_vrsn_cd_25   = PVSNCD01-PVSNCD25
		   prcdrdt1-prcdrdt25                    		  = PROCDT1-PROCDT25
		   dgns_e_vrsn_cd_1-dgns_e_vrsn_cd_12			  = EVRSCD01-EVRSCD12

			bene_id  	= hicno 
			medparid	= hse_unique_id
			prvdrnum	= PROVID
			admsndt  	= ADMIT
			type_adm 	= TYPEADM
			dschrgdt 	= DISCH
			dschrgcd 	= DISST
			dstntncd 	= DDEST
			clm_type 	= NCH_CLM_TYPE_CD
			crnry_cd 	= CCUIND
			icuindcd 	= ICUIND
			qlfyfrom 	= from_dt
			qlfythru	= thru_dt
			drg_cd      = DRGCD
			MS_CD       = MSCD
			state_cd    = PSTATE_ALPHA;

	CASE_TYPE = "CMS";

	/*Make a Post Acute Care file and an Inpatient file*/
	if 3025<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=3099 | spclunit in("R","T") then factype="IRF";
	if 5000<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=6499 then factype="SNF";
	if 2000<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=2299 then factype="LTC";
	if 1<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=899 & spclunit ~in("R","T","S","M") then factype="STA"; 
	if factype in("IRF","SNF","LTC") then output out.pta_in_base_dataset_pac;

	/*Stay is inpatient, not PAC*/
	if 1<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=899 then output out.pta_in_base_dataset;
run;

/*Read in the HHA and Carrier Datasets - Add them to the base_dataset_pac file*/
%macro hha_op(start_yr, end_yr);
	%do i = &start_yr. %to &end_yr.;

		/*Different years require different renaming. Early years require special xwalked datasets that map BID to
		  bene_id, which gets used from 2006 onwards.*/

		%if &i.>=2003 & &i.<=2005 %then %do;
			libname xw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/b2006_2008/Request863";
			
			/*HHA*/
			libname hha&i. "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/cms7707/DATA";

			proc contents data=hha&i..hha&i._; run;

			data hha&i. (keep=BID PROVID /*PPS_IND*/ ADMIT DISCH PMT_AMT TOT_CHG DDEST FACTYPE CASE_TYPE);
				set hha&i..hha&i._ (where=(BID_~=""));
				rename  provno=provid
						/*pps_ind=*/
						pmt_amt=pmt_amt
						tot_chg=tot_chg
						stus_cd=DDEST
						BID_=BID;
				factype="HHA";
				CASE_TYPE = "CMS";

				admyr=substr(from_dt,1,4);
				admmn=substr(from_dt,5,2);
				admdy=substr(from_dt,7,2);

				disyr=substr(thru_dt,1,4);
				dismn=substr(thru_dt,5,2);
				disdy=substr(thru_dt,7,2);

				ADMIT=mdy(admmn,admdy,admyr);
				DISCH=mdy(dismn,disdy,disyr);

				format ADMIT DISCH DATE9.;
			run;

			/*Merge on the BID --> Bene_ID crosswalk*/
			data xwalk;
				set xw.bid_bene_xwalk (where=(BID~=""));
			run;

			proc sort data=hha&i.; by BID; run;
			proc sort data=xwalk nodupkey; by BID; run;

			data hha&i. (drop=BID rename=(bene_id=hicno));
				merge hha&i. (in=a)
					  xwalk (in=b);
				by BID;
				if a & b;
			run;


			/*SNF-IRF-LTC*/

			/*Extract IRF and SNFs from the 2003-2005 RAND medpar files. Different than the datasets we used to get acute data*/
			libname sil "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/cms7707/DATA";

			proc contents data=sil.medpar&i._; run;
			proc print data=sil.medpar&i._(obs=20); run;

			data snf_irf_ltc&i. (keep=BID PROVID /*PPS_IND*/ ADMIT DISCH PMT_AMT TOT_CHG FACTYPE CASE_TYPE provcode los miss_los miss_disch);
				set sil.medpar&i._;
				if 3025<=(input(substr(provno,length(provno)-3,4),?? 4.))<=3099 | provcode in("R","T") then factype="IRF";
				if 5000<=(input(substr(provno,length(provno)-3,4),?? 4.))<=6499 then factype="SNF";
				if 2000<=(input(substr(provno,length(provno)-3,4),?? 4.))<=2299 then factype="LTC";
				if factype in("IRF","SNF","LTC");
				CASE_TYPE = "CMS";

				rename provno=provid
					   totchrg=tot_chg
					   BID_=BID;
				/*Dates in this file take the form YYYYDDD - the year followed by how many days into the year, e.g. 2005036 is 36 days into 2005 (feb 5, 2005)*/
				adm_days=input(substr(trim(left(admdte)),length(trim(left(admdte)))-2,3),8.);
				adm_yr=input(substr(admdte,1,4),8.);
				temp_adm=mdy(1,1,adm_yr);
				admit=temp_adm+adm_days-1; /*Take the start of the year, plus the number of days into that year, minus 1. AKA january 2nd, 2005 would be 2005jan01 + 2 - 1. */

				dis_days=input(substr(trim(left(disdte)),length(trim(left(disdte)))-2,3),8.);
				dis_yr=input(substr(disdte,1,4),8.);
				temp_dis=mdy(1,1,dis_yr);
				disch=temp_dis+dis_days-1; /*Take the start of the year, plus the number of days into that year, minus 1. AKA january 2nd, 2005 would be 2005jan01 + 2 - 1. */

				miss_los=los=.;
				miss_disch=admit~=. & disch=.;

				format ADMIT DISCH DATE9.;
			run;

			title "Check LOS for SNFs with no discharge";
			proc freq data=snf_irf_ltc&i.;
				tables miss_los / missing;
				where factype="SNF";
			run;
			title;

			/*Merge on the BID --> Bene_ID crosswalk*/
			proc sort data=snf_irf_ltc&i.; by BID; run;

			data snf_irf_ltc&i. (drop=BID rename=(bene_id=hicno));
				merge snf_irf_ltc&i. (in=a)
					  xwalk (in=b);
				by BID;
				if a & b;
			run;

		%end;

	    /*2006-2008*/
		%if 2006<=&i. & &i.<=2008 %then %do;
			libname hha&i. "&hha.";
			libname op&i. "&op.";

			/*HHA*/
			data hha&i. (keep=HICNO DIAG1 PROVID /*PPS_IND*/ ADMIT DISCH PMT_AMT TOT_CHG DDEST FACTYPE CASE_TYPE);
				set hha&i..hha_base_claims_j&i. (where=(bene_id~=""));
				rename  DGNSCD1=DIAG1
						provider=PROVID
						/*pps_ind=*/
						from_dt=ADMIT
						thru_dt=DISCH
						pmt_amt=pmt_amt
						tot_chg=tot_chg
						stus_cd=DDEST
						bene_id=hicno;
				factype="HHA";
				CASE_TYPE = "CMS";
			run;
		%end;
	
		/*2009+*/
		%if &i.>=2009 %then %do;
			libname hha&i. "&hha.";
			libname op&i. "&op.";

			/*HHA*/
			data hha&i. (keep=HICNO DIAG1 PROVID /*PPS_IND*/ ADMIT DISCH PMT_AMT TOT_CHG DDEST FACTYPE CASE_TYPE);
				set hha&i..hha_base_claims_j&i. (where=(bene_id~=""));
				rename  icd_dgns_cd1=DIAG1
						provider=PROVID
						/*pps_ind=*/
						from_dt=ADMIT
						thru_dt=DISCH
						pmt_amt=pmt_amt
						tot_chg=tot_chg
						stus_cd=DDEST
						bene_id=hicno;
				factype="HHA";
				CASE_TYPE = "CMS";
			run;
		%end;
	%end;
%mend;
%hha_op(&syr.,&eyr.);

/* Keep this for now for historical purposes, but the MDS has been updated below*/
/*data mds (keep=hicno admit disch factype disch_yr);
	set mds.mds2_2003_2010 (in=a where=(first_case=1) rename=(bene_id=hicno))
		mds.mds3_2011_2013 (in=b where=(A2000_DSCHRG_DT~="") rename=(bene_id=hicno));
	ADMIT=ENTRY_DT;
	DISCH=DISCH_DT;
	FACTYPE="MDS";
	disch_yr=year(disch);
run;
*/

/* BR 2-6-17. This is the new MDS with the simple smoothing algorithm between MDS2 and MDS3. Created from an MDS program in the HRRP folder.*/
data mds (keep=hicno admit disch factype disch_yr);	
	set mds.mds_stays_2006_2014_1 (in=a where=(first_case=1) rename=(bene_id=hicno));
	ADMIT=admit_dt;
	DISCH=disch_dt;
	FACTYPE="MDS";
	disch_yr=year(disch);
run;

proc freq data=mds;
	tables disch_yr;
run;

data out.pta_in_base_dataset_pac;
	set out.pta_in_base_dataset_pac
		snf_irf_ltc2003-snf_irf_ltc2005 /*This is separate from pta_in_base_dataset_pac because RAND gave us separate medpar files for acute and PAC from 2003-2005*/
		hha&syr.-hha&eyr.
		mds;
	test_disch=year(disch);
run;

proc sort data=out.pta_in_base_dataset; by HICNO ADMIT DISCH; run;

data out.pta_in_base_dataset;
	set out.pta_in_base_dataset;
	year_disch=year(disch);
run;

proc freq data=out.pta_in_base_dataset;
	tables year_disch;
run;

proc freq data=out.pta_in_base_dataset_pac;
	tables factype*year_disch;
run;

data out.pta_in_base_dataset;
	set out.pta_in_base_dataset (drop=year_disch);
run;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-9-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%macro loop(factype);
	%do i=&syr. %to &eyr.;
		%nobs(data=&factype.&i.,name=obs); 
	%end;
%mend;
%loop(factype=med);
%loop(factype=hha);
%nobs(data=out.mds.mds_stays_2006_2014_1,name=obs); 
%nobs(data=out.pta_in_base_dataset,name=obs); 
%nobs(data=out.pta_in_base_dataset_pac,name=obs); 



