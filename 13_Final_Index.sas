/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 6_Final_Index

FOR USE WITH: AMI, HF, PNEUMPONIA, STROKE AND COPD

PURPOSE:      create final readmission and mortality index files 

OVERVIEW:     use pstindex to create POST_FLAG and POST_INDEX_STAY in readmission index file.
			  use readmission index to add year to post index. 

INPUT DATA: 
	dx_data.index03   *PRE-SORTED BY HICNO CASE
	dx_data.pstindex  *PRE-SORTED BY HICNO -for creating POST_FLAG and POST_INDEX_STAY flags in the index file.

OUTPUT FILES: 
	An_files.readmissions_index_&DX._&YY.&YYE.
	An_files.mortality_index_&DX._&YY.&YYE.
	An_files.deleted_mort_&DX._&YY.&YYE.

************************************************************************************************************/

OPTIONS COMPRESS=YES;

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
PUT "  Job Name: 6_Final_Index" /;
PUT "  Start Date: " StartDate ;
PUT "  Start Time: " StartTime ;
PUT "===================================================================================";
RUN;

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";


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
%remove(ds=An_files.readmissions_index_&DX._&YY.&YYE.);
/**********************************************************************************************
**********************************************************************************************/

**********************************************************************
* CHECK MACRO VARIABLES
**********************************************************************;
%PUT DX (MEASURE VARIABLE)= &DX.;

%PUT DXN (MAX NUMBER OF DIAGNOSIS)= &DXN.;
%PUT PRN (MAX NUMBER OF PROCEDURE)= &PRN.;

%PUT YY (FIRST YEAR OF SPLIT YEAR PERIOD)= &YY.;
%PUT YYE (LAST YEAR OF SPLIT YEAR PERIOD)= &YYE.;
**********************************************************************;

**********************************************************************
* CREATE FINAL READMISSION INDEX DATASET
* SELECT MORTALITY INDEX RECORDS AND SPLIT OUT TRANSFERS TO CHECK ELIGIBILITY
* ASSIGN POST FLAGS, RECODE TRANS FIELDS AND DEFINE VA_OBS_STAY
**********************************************************************;
DATA An_files.readmissions_index_&DX._&YY.&YYE.
		(KEEP = admit admsour birth bstate case ccuind county ddest death diag1-diag&DXN. disch disst
			drgcd dvrsnd01-dvrsnd25 edbvdeth edgscd01-edgscd12 evrscd01-evrscd12
			group hicno history_case hsp_enr_on_admit hsp_enr_on_disch hsp_enr_prior_admit 
			hsp_enr_prior_disch icuind los mdcl_rec /*mscd*/ npi_at npi_op para para_b 
			poaend01-poaend12 poancd1-poancd25 post_flag post_index_stay postmo postmo_a
			postmod postmod_a premo premo_a proc1-proc&PRN. procdt1-procdt&PRN. provid pstate
			pvsncd01-pvsncd25 race sex trans trans_first trans_last trans_mid txflag
			typeadm unreldmg unreldth upin_at upin_op va_obs_stay year zip factype sslssnf pmt_amt tot_chg 
			RTI_RACE_CD SORTEDBY=hicno case)
	 An_files.mortality_index_&DX._&YY.&YYE.
		(KEEP = admit admsour birth bstate case ccuind county ddest death diag1-diag&DXN. disch disst
			drgcd dvrsnd01-dvrsnd25 edbvdeth edgscd01-edgscd12 evrscd01-evrscd12
			group hicno history_case hsp_enr_on_admit hsp_enr_on_disch hsp_enr_prior_admit 
			hsp_enr_prior_disch icuind los mdcl_rec /*mscd*/ npi_at npi_op para para_b 
			poaend01-poaend12 poancd1-poancd25 post_flag post_index_stay postmo postmo_a
			postmod postmod_a premo premo_a proc1-proc&PRN. procdt1-procdt&PRN. provid pstate
			pvsncd01-pvsncd25 race sex trans trans_first trans_last trans_mid txflag
			typeadm unreldmg unreldth upin_at upin_op va_obs_stay year zip factype sslssnf pmt_amt tot_chg
			RTI_RACE_CD SORTEDBY=hicno case)
	 An_files.deleted_mort_&DX._&YY.&YYE.
		(KEEP = admit admsour birth bstate case ccuind county ddest death diag1-diag&DXN. disch disst
			drgcd dvrsnd01-dvrsnd25 edbvdeth edgscd01-edgscd12 evrscd01-evrscd12
			group hicno history_case hsp_enr_on_admit hsp_enr_on_disch hsp_enr_prior_admit 
			hsp_enr_prior_disch icuind los mdcl_rec /*mscd*/ npi_at npi_op para para_b 
			poaend01-poaend12 poancd1-poancd25 post_flag post_index_stay postmo postmo_a
			postmod postmod_a premo premo_a proc1-proc&PRN. procdt1-procdt&PRN. provid pstate
			pvsncd01-pvsncd25 race sex trans trans_first trans_last trans_mid txflag
			typeadm unreldmg unreldth upin_at upin_op va_obs_stay year zip factype sslssnf pmt_amt tot_chg 
			RTI_RACE_CD SORTEDBY=hicno case)
	 post_flag_check (KEEP = post_flag post_index_stay ptrans trans ptrans_first trans_first 
						     ptrans_mid trans_mid ptrans_last trans_last mort_flag);
	MERGE dx_data.index03(IN=I)
		  dx_data.pstindex(IN=pst);
	BY hicno;
	IF I;


	/*ADDITIONAL VARIABLES FOR AMI, HF AND PN*/
	/*FORMAT case_type diag26-diag41 $8.;*/
	/*case_type='CMS';*/

	/*INITIALIZE POST FLAGS*/
	post_flag = 0;
	post_index_stay = 0;

	/*CREATE COMPARISON FLAGS TO CHECK CHANGES*/
	ptrans = trans;
	ptrans_first = trans_first;
	ptrans_mid = trans_mid;
	ptrans_last = trans_last;

	/*FLAG IF SAME OR NEXT DAY ADMIT AND DIFFERENT PROVIDER*/
	IF pst AND 0 <= postadmit - disch <= 1
		   AND provid NE postprovid 
	  THEN DO;
		post_flag = 1;
		trans = 1;

		/*FLAG POST_INDEX_STAY IF POST INDEX CASE IS A QUALIFIED DIAGNOSIS*/
		IF index_stay = 1 THEN post_index_stay = 1;

		/*CHANGE TRANS_LAST CASE TO TRANS_MID*/
		IF trans_last = 1 THEN DO;
			trans_mid = 1;
			trans_last = 0;
		END;
		/*CHANGE NON TRANSFER CASE TO TRANS_FIRST*/
		ELSE trans_first = 1;

		/*OUTPUT CHECK DATASET*/
		OUTPUT post_flag_check;
	END;


	/*REFORMAT VARIABLES TO MATCH FINAL LAYOUT*/
	LENGTH	ddest2 ccuind2 drgcd2 icuind2 los2 /*mscd2*/ race2 3 provid2 pstate2 upin_at2 upin_op2 $6.
			nprocdt1-nprocdt&PRN. $8.;

	ddest2 = input(ddest, 3.);
	ccuind2 = ccuind;
	drgcd2 = input(drgcd, 3.);
	icuind2 = icuind;
	los2 = los;
	/*mscd2 = input(mscd, 3.);*/
	provid2 = substr(left(provid), 1, 6);
	pstate2 = substr(left(pstate), 1, 6);
	upin_at2 = substr(left(upin_at),1,6);
	upin_op2 = substr(left(upin_op),1,6);
	race2 = input(race, 3.);

	ARRAY PROC procdt1-procdt&PRN.;
	ARRAY NPRC nprocdt1-nprocdt1&PRN.;

	DO I = 1 TO &PRN.; 
		IF PROC(I) = . THEN NPRC(I)='';
		ELSE NPRC(I) = put(PROC(I),YYMMDDN8.);
	END;

	DROP ddest ccuind drgcd icuind los /*mscd*/ provid pstate upin_at upin_op race 
		 procdt1-procdt&PRN.;

	RENAME	ddest2		= ddest
			ccuind2		= ccuind
			drgcd2		= drgcd 
			icuind2		= icuind 
			los2		= los
			/*mscd2		= mscd*/
			provid2		= provid
			pstate2		= pstate
			upin_at2	= upin_at
			upin_op2	= upin_op
			race2		= race
			nprocdt1-nprocdt&PRN. = procdt1-procdt&PRN.;


	/*DEFINE VA OBSERVATION STAY*/
	va_obs_stay=0;



	LABEL
	/*DEFINE LABELS FOR INDEX FIELDS*/
		admit				='Admission date'
		admsour				='Source of admission'
		birth				='EDB date of birth'
		bstate				='Beneficiary State of Residence'
		case				='Case number marker'
		ccuind2				='Coronary Care Unit indicator'
		county				='Beneficiary County of Residence'
		ddest2				='Discharge destination code'
		death				='EDB date of death verified' 
		disch				='Discharge date'
		disst				='Medicare discharge status code'
		drgcd2				='Diagnosis Related Group'
		edbvdeth			='EDB Verified DOD Switch'
		group				='Transfer bundle indicator'
		hicno  				='Clean xrefd ID'
		history_case		='Revised case number for merging index and history files'
		hsp_enr_on_admit    ='CMS hospice enrollment on index admission date'
		hsp_enr_on_disch    ='CMS hospice enrollment on index discharge date'
		hsp_enr_prior_admit ='CMS hospice enrollment prior to index admission'
		hsp_enr_prior_disch ='CMS hospice enrollment prior to index discharge'
		icuind2				='Intensive Care Unit indicator'
		los2				='Length of stay'
		mdcl_rec			='Medical record number'
		/*mscd2				='Denom medicare status code'*/
		npi_at				='NPI of attending physician'
		npi_op				='NPI of operating physician'
		para				='Part A FFS enrollee at admission'
		para_b				='Part A and Part B FFS enrollee at admission'
		post_flag       	='Flag for transfers out at end of period'
		post_index_stay 	='1=Transfer in at end of study period had qualifying dx'
		postmo				='Post-admission completeness indicator'
		postmo_a			='Post-admission completeness indicator - PTA only'
		postmod				='Post-discharge completenes indicator'
		postmod_a			='Post-discharge completeness indicator - PTA only'
		premo				='Pre-admission completeness indicator'
		premo_a				='Pre-admission completeness indicator - PTA only'
		provid2				='Medicare provider number'
		pstate2				='Medicare Provider state code'
		race2				='Bene race'	
		sex					='Beneficiary sex from edb'                                         
		trans				='1=stay is part of a transfer chain'
		trans_first			='First stay in a transfer chain'
		trans_last			='Last stay in a transfer chain'
		trans_mid			='Middle stay in a transfer chain'
		txflag 				="-1=transfer, 1=died, 0=otherwise"
		typeadm				='Type of admission'
		unreldmg 			='Unreliable age/gender indicator'                                  
		unreldth 			='Unreliable death indicator'                                       
		upin_at2			='UPIN of attending physician'
		upin_op2			='UPIN of operating physician'
		va_obs_stay			='VA flag for observation stay'
		year				='Year cohort'
		zip					='Beneficiary Zipcode of Residence'
		%MACRO LABELCOUNTS(); /*USE LOOPS TO CREATE LABELS FOR SEQUENTIAL VARIABLES*/
			%DO I=1 %TO &DXN.;
				diag&I		="Diagnosis code #&I"
			%END;

			%DO I=1 %TO 25;
				%LET N=%SYSFUNC(PUTN(&I,Z2.));
				poancd&I	="Diagnosis code present on admission indicator"
				dvrsnd&N	="Diagnosis code version indicator"
				pvsncd&N	="Procedure code version indicator"
			%END;

			%DO I=1 %TO 12;
				%LET N=%SYSFUNC(PUTN(&I,Z2.));
				edgscd&N	="External cause of injury diagnosis code"
				evrscd&N	="Diagnosis code version indicator"
				poaend&N	="External cause of injury POA indicator"
			%END;

			%DO I=1 %TO &PRN.;
				proc&I		="Procedure code #&I"
				nprocdt&I	="Procedure date #&I"
			%END;
		%MEND LABELCOUNTS;
		%LABELCOUNTS;
	;

	/*OUTPUT ALL RECORDS FOR THE READMISSION INDEX*/
	OUTPUT An_files.readmissions_index_&DX._&YY.&YYE.;

	/*SELECT MORTALITY INDEX RECORDS*/
	IF mort_flag = 1 THEN OUTPUT An_files.mortality_index_&DX._&YY.&YYE.;

	/*SELECT DELETED MORTALITY RECORDS*/
	ELSE IF mort_flag NE 1 THEN DO;
		exclusion=2;  /*OUT OF RANGE CASES ARE EXCLUDED IN PROGRAM 1*/
		OUTPUT An_files.deleted_mort_&DX._&YY.&YYE.;
	END;
RUN;


TITLE 'CHECK POST FLAG CHANGES';
PROC FREQ DATA=post_flag_check;
	TABLE mort_flag*post_flag*post_index_stay*ptrans*trans*ptrans_first*trans_first*ptrans_mid*trans_mid*ptrans_last*trans_last /LIST MISSING;
RUN;

TITLE "An_files.readmissions_index_&DX._&YY.&YYE. CONTENTS";
PROC CONTENTS DATA=An_files.readmissions_index_&DX._&YY.&YYE.;
RUN;


TITLE "An_files.mortality_index_&DX._&YY.&YYE. CONTENTS";
PROC CONTENTS DATA=An_files.mortality_index_&DX._&YY.&YYE.;
RUN;

TITLE 'VALIDATE DIAG1 FOR MORTALITY ONLY INCLUDES MEASURE DIAGS';
PROC FREQ DATA=An_files.mortality_index_&DX._&YY.&YYE.;
	TABLE diag1 /LIST MISSING OUT= mort_diags;
RUN;

DATA check_mort_diags;
	SET mort_diags;
	IF NOT (&&DIAGSELECT_&DX);
RUN;

TITLE 'MORTALITY INDEX DIAG1 NOT IN MEASURE DIAG LIST - THERE SHOULD BE NONE';
PROC FREQ DATA=check_mort_diags;
	TABLE diag1 /LIST MISSING;
RUN;

TITLE "An_files.deleted_mort_&DX._&YY.&YYE. CONTENTS";
PROC CONTENTS DATA=An_files.deleted_mort_&DX._&YY.&YYE.;
RUN;
**********************************************************************;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-9-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,8.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=dx_data.index03,name=obs); 
%nobs(data=An_files.readmissions_index_&DX._&YY.&YYE.,name=obs); 


 /*DELETE WORK DATASET TO CLEAN UP THE EG PROCESS FLOW*/
PROC DELETE DATA=post_flag_check mort_diags check_mort_diags; 
RUN;
TITLE;

DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 6_Final_Index" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
