	*****************************************************************;
	* For SK 30-day readmission measure using Medicare claim data.  *;
	* Input data must conform to the specifications of R&A.         *;
	* This is a new version with applying History_CASE              *;                                                
	* SAS 9.3 WIN                                                   *;
	* YALE/YNHH CORE                                 *;
    * SK measure  03-12-2013                                        *;
   
	** Other updates are made on 03/13/2013  by CW
    -Add new planned readmission algorithm                                                
    -Correct Maryland fix sort issue      
    -Change the # of Diagnosis Codes to 25 and add E-codes
    -Change the # of Procedure Codes to 25
    -Maryland rehab/psych change  

    -Removal of ICD-9-CM code 436 from Stroke study cohort
     Changqin Wang    03-15-2013

    ** Upadtes in 2014  by CW on 04/09/2014
    - updated planned readmission with version3.0
    - Took out "RANDOM _RESIDUAL_" from proc GLIMMIX
    - Added "gamma' for the hospital-specific effect in output file

	*****************************************************************;

 	OPTIONS SYMBOLGEN MPRINT; /* leave this on for program checking purpose */
	*****************************************************************************;
	* SPECIFY VARIOUS FILE PATHES, DISEASE CONDITION, AND YEAR                  *;
	*****************************************************************************;

libname mds "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/Temp";
libname ccr "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/Cost_Reports";
%let include = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

%let year=&YY.&YYE.; /* e.g., 0910, or 11  */;
%let loe=30;
%LET CONDITION=&DX.; 
%let PRE_COV=Y; /*Y or N, determines if inclusion criteria demands 12 months of consecutive coverage before admission. BR 7/8/15*/
%LET PATH1= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final  ; /*RAW DATA FILES PATH, MUST BE CHANGED */

%LET PATH4= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final ; /* for derived data sets, MUST BE CHANGED */
%LET PATH5= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
			/* for SAS macros and HCC FORMAT CATALOG RESIDES, MUST BE CHANGED  */

/*Import the transfer file formats for CCMap and CCS*/
%let path6 = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/HRRP/CCMap2008_2012; *CC Map directory;
%let path7 =/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/HRRP/CCS_2013; *CCS Format DIrectory;
%let fmt = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/HRRP/CC_and_CCS_FORMATS_TRN;
libname fmt "&fmt";
libname F "&path6";
libname C "&path7";
filename datfile "&fmt./CC2008_2012.TRN";
filename datfile2 "&fmt./CCS2013.TRN";
proc cimport lib=F infile=datfile;
run;
proc cimport lib=C infile=datfile2;
run;

LIBNAME RAW "&PATH1"; 
LIBNAME R "&PATH4"; 

/*Formats taken care of above - code changed to deal with transport file format catalog BR*/
*LIBNAME F "&PATH7"; 
*LIBNAME C "&PATH6"; 
OPTIONS FMTSEARCH=(F C) nofmterr;

    %INCLUDE "&PATH5./Readmission_Macros_2014_SK.sas";

	%LET ADMISSION=RAW.READMISSIONS_INDEX_&CONDITION._&YEAR; 
    %LET POST=RAW.POSTINDEX_&CONDITION._&YEAR; 
    %LET HXDIAG=RAW.DIAGHISTORY_&CONDITION._&YEAR;
    %LET HXPROC=RAW.prochistmm_&CONDITION._&YEAR; 

    %LET ALL=R.&CONDITION._READM_ALL_&year.;
    %LET ANALYSIS=R.&CONDITION._READM_ANALYSIS_&year.;
    %LET RSRR=R.&CONDITION._READM_RSRR_&year.;
    %LET RESULTS=R.&CONDITION._READM_RSRR_BS;
    %LET EST=R.&CONDITION._READM_EST_&year.;

	/* SK READMISSION MODEL VARIABLES */

%let MODEL_VAR=
age_65
MALE
CANCER
DEMENTIA
PARALYSIS_FUNCTDIS
CEREB_HEMORR
ISCHEMIC_STROKE
PRE_ART_CEREB
HEMI_PLEGIA
DECUBITUS_ULCER
DIABETES
MALNUTRITION
IRON_DEFICIENCY
CHF
OBESITY
RENAL_FAILURE
COPD
DIS_FLUID
ESRD_DIALYSIS
SEIZURE
MCANCER
OTHER_UTD
HEMATOLOGICAL
MAJ_SYMPTOMS
HYPER_HEART_DX
VASDIS_WCOMP 
OTHERLUNG;


	%LET CDIAG=drgcd in(61,62,63,64,65,66); 
					/* removal of code 436 by stroke group. CW 03-15-2013 */ 

	%LET TRANS_DIAG=R.DIAGHISTORY_&CONDITION._&YEAR._TRANS;
	%LET TRANS_PROC=R.PROCHISTORY_&CONDITION._&YEAR._TRANS;

	%RETRIEVE_HX(&ADMISSION, &CDIAG, &TRANS_DIAG, &TRANS_PROC);

data INDEX;
	set &ADMISSION (IN=A WHERE=(PARA=1)); 

	IF drgcd in(61,62,63,64,65,66) THEN SK_DRG_CASE=1;
	ELSE SK_DRG_CASE=0;

RUN; 

proc contents data=index; run;

proc print data=index (obs=10); 
	var drgcd;
run;

proc freq data=index;
	tables SK_DRG_CASE / missing;
run;
			
/* ELIMINATE ADMISSIONS THAT APPEAR TWICE (ACROSS YEARS) */

PROC SORT DATA=INDEX NODUPKEY DUPOUT=QA_DupOut EQUALS;
	BY HICNO ADMIT DISCH PROVID;
RUN;

/* IDENTIFY AND COMBINE TWO ADJACENT Stroke ADMISSIONS (disch1=admit2), USE DISCHARGE DATE
	OF 2ND ADMISSION TO REPLACE DISCHARGE DATE OF 1ST ADMISSION (disch1=disch2), 
	SAME FOR DISCHARGE STATUS, TRANS_FIRST, TRANS_MID, POSTMOD. 
	ALSO, CREATE CASE_P TO BE USED FOR FINDING READMISSION.  
	THIS WORKS WHEN THERE ARE MORE THAN TWO ADJACENT SK ADMISSIONS. */

DATA TEMP; 
	SET INDEX;
	BY HICNO;
if (admit <= lag(disch) <= disch) and lag(provid)=provid
	and lag(hicno)=hicno
	and lag(SK_DRG_CASE)=SK_DRG_CASE=1 then combine0=1;
else combine0=0;
RUN;

proc sort data=TEMP;
	by hicno descending admit descending disch;
run;

data TEMP2 QA_CombOut_mid;
set TEMP;
by hicno;

if (admit <= lag(admit) <= disch) and 
	lag(provid)=provid
	and lag(hicno)=hicno
	and lag(SK_DRG_CASE)=SK_DRG_CASE=1 then combine=1;
else combine=0;
if combine0 and combine then output QA_CombOut_mid;
else output TEMP2;

run;

data TEMP3 QA_CombOut_last;
set TEMP2;

disch_2=lag(disch);
case_2=lag(case);
ddest_2=lag(ddest);
trans_first_2=lag(trans_first);
trans_mid_2=lag(trans_mid);
postmod_2=lag(postmod);
if lag(provid)=provid and lag(hicno)=hicno and lag(combine0)=1 then do;
	disch=disch_2;
	case_p=case_2;
	ddest=ddest_2;
	trans_first=trans_first_2;
	trans_mid=trans_mid_2;
	postmod=postmod_2;
	end;
else case_p=case;

drop disch_2 case_2 ddest_2 trans_first_2 trans_mid_2 postmod_2;

if combine0 ^=1 then output TEMP3;
else output QA_CombOut_last;

run;

PROC SORT DATA=TEMP3;
	BY HICNO DESCENDING ADMIT  DESCENDING DISCH PROVID;
RUN;

/* APPLY THE FOLLOWING INCLUSION AND EXCLUSION CRITERIA:
	SK_DRG_CASE=1, AGE >=65, DEAD=0, PREMO=12, POSTMOD=1, 2, 3,
	TRANS_COMBINE=0, AMA=0 */

DATA ALL; 
	SET TEMP3 (DROP=COMBINE0);
	BY HICNO;

ATTRIB TRANSFER_OUT LABEL='TRANSFER OUT' LENGTH=3.;
ATTRIB TRANS_COMBINE LABEL='TRANSFER OUT' LENGTH=3.;
ATTRIB DD&loe. LABEL="&loe.-DAY MORTALITY FROM DISCHARGE" LENGTH=3.;
ATTRIB AGE_65 LABEL='YEARS OVER 65' LENGTH=3.;
ATTRIB AGE65 LABEL='AGE GE 65' LENGTH=3.;
ATTRIB DEAD LABEL='IN HOSPITAL DEATH' LENGTH=3.;
ATTRIB CRITERIA LABEL='MEET INC & EXL CRITERIA' LENGTH=3.;

/* TRANSFER OUT INDICATOR, SAME PTS, DIFFERENT HOSP, 0 OR 1 DAY IN BETWEEN, NOT V CODE VISIT */
TRANSFER_OUT=(ADMIT <= LAG(ADMIT) <= DISCH +1) AND (HICNO=LAG(HICNO)) AND (PROVID ^=LAG(PROVID)) AND 
			 factype ~in("IRF","SNF","LTC","HHA","OUT","MDS");
*IF SUBSTR(DIAG1, 1, 1)='V' OR SUBSTR(LAG(DIAG1), 1, 1)='V' THEN TRANSFER_OUT=0;

/* add post_flag to account for possible transfer that is outside of study period. 
	1/11/2010. ZQ */

TRANS_COMBINE=(TRANSFER_OUT OR TRANS_FIRST OR TRANS_MID or post_flag);

MALE=(SEX=1);
AGE=INT((ADMIT - BIRTH)/365.25);
AGE65=(AGE >=65);
AGE_65=AGE - 65;

DEAD=(DDEST=20);
AMA=(DDEST=7);

DD180=0;
IF DEATH ^=. THEN DO;
	IF 0 < (DEATH - DISCH) <=&loe. THEN DD&loe.=1;
	ELSE IF (DEATH - DISCH) > &loe. THEN DD&loe.=0;
	ELSE IF (DEATH - DISCH) <= 0 THEN DD&loe.=0;
	IF 0 < (DEATH - DISCH) <=180 THEN DD180=1;
	END;
ELSE DD&loe.=0;

IF DD180=1 THEN POSTMOD=6;

/*Toggle the 12 months of prior coverage inclusion criteria on and off. Brendan Rabideau 7/8/15*/
%macro prior_cov;
	%IF "&PRE_COV." ~="Y" & "&PRE_COV."~="N" %THEN 
		PUTLOG 'ERROR: PLEASE INDICATE WHETHER 12 MONTHS PRIOR COVERAGE IS REQUIRED, &PRE_COV = Y/N';
	%ELSE %IF "&PRE_COV." = "Y" %THEN PRIOR12=(PREMO=12);
	%ELSE %IF "&PRE_COV." = "N" %THEN PRIOR12=1;
%mend;
%prior_cov;

POST1=(POSTMOD IN (6));  /* take out dead 3/30/11 */

/* INCLUSIOIN CRITERIA: FFS, HF CASE, AGE GE 65, WITH 12-MONTH HISTORY,
   EXCLUSION  CRITERIA: TRANSFER OUT, IN HOSPITAL DEATH, WITHOUT >= 1 MOTNH POST,
						AMA */

IF (SK_DRG_CASE=1) AND (TRANS_COMBINE=0) AND (AGE65=1) AND (DEAD=0)	
		AND (PRIOR12=1) AND (POST1=1) AND AMA=0 THEN CRITERIA=1;
ELSE CRITERIA=0;

RUN;

proc freq data=all;
	tables SK_DRG_CASE TRANS_COMBINE AGE65 DEAD PRIOR12 POST1 / missing; 
run;


/* IDENTIFY # OF ADMISSIONS PER PATIENT */
PROC SQL;
	CREATE TABLE STEP1WCOUNT AS
	SELECT DISTINCT HICNO, COUNT(HICNO) AS ADM_COUNT
	FROM ALL (WHERE=(CRITERIA=1))
	GROUP BY HICNO;
QUIT;


/* CREATING 2 FILES, ONE FOR PTS WITH 1 ADM, & ANOTHER FOR PTS WITH 1+ ADMS */
DATA ONE (DROP=ADM_COUNT) ONEPLUS (DROP=ADM_COUNT);
	MERGE ALL (WHERE=(CRITERIA=1)) STEP1WCOUNT;
	BY HICNO;

KEEP HICNO CASE CASE_P ADMIT DISCH ADM_COUNT YEAR;

IF ADM_COUNT=1 THEN OUTPUT ONE;
ELSE IF ADM_COUNT > 1 THEN OUTPUT ONEPLUS;

RUN;

PROC SORT DATA=ONEPLUS;
	BY HICNO ADMIT DISCH;
RUN;

/* KEEP ADMS THAT ARE AT LEAST &loe. DAYS APART */
DATA ONEPLUS&loe.DAY;
	SET ONEPLUS;
	BY HICNO;
RETAIN BASEDATE 0;
IF FIRST.HICNO THEN DO;
	BASEDATE=DISCH;
	KEEP=1;
	DIFF=0;
	END;
ELSE IF (ADMIT - BASEDATE) <=&loe. THEN DO;
	KEEP=0;
	DIFF=ADMIT - BASEDATE;
	END;
ELSE IF (ADMIT - BASEDATE) > &loe. THEN DO;
	KEEP=1;
	DIFF=ADMIT - BASEDATE;
	BASEDATE=DISCH;
	END;

IF KEEP=1;

DROP BASEDATE;

RUN;

DATA INDEXADMISSION;
	SET ONE ONEPLUS&loe.DAY;
RUN;

PROC SORT DATA=INDEXADMISSION;
	BY HICNO CASE;
RUN;

PROC SORT DATA=ALL;
	BY HICNO CASE;
RUN;

DATA _ALL;
MERGE ALL (IN=A WHERE=(SK_DRG_CASE=1)) INDEXADMISSION (IN=B KEEP=HICNO CASE);
BY HICNO CASE;

IF A;

ATTRIB _&loe.DAYS LABEL="&loe. DAYS WINDOW EXCLUSION" LENGTH=3.;
ATTRIB SAMPLE LABEL='ADMISSION IN FINAL SAMPLE' LENGTH=3.;

if criteria=1 and not b then _&loe.Days=1;
else _&loe.Days=0;

IF B THEN SAMPLE=1;
ELSE SAMPLE=0;

RUN;

PROC SORT DATA=_ALL;
BY HICNO CASE_P;
RUN;

 
DATA POST;
SET &POST (IN=A);
if a then CASE_P=CASE;
length   i j k 3.;
 
***** determine the Proc CCS group each procedure falls into ******;
ATTRIB procccp_1-procccp_25  LENGTH=$3.;
ARRAY procccp_ (1:25) $ procccp_1-procccp_25;
ARRAY procccsp_(1:25) $  PROC1 - PROC25;

DO k=1 TO 25;
procccp_(k) = put(procccsp_(k),$ccsproc.); 
end;

****** Categorize the CCS Diagnosis Claims for the potential readmissions *******;
DCGDIAG = diag1;
ADDXG_p = PUT(DCGDIAG,$CCS.);

*****THIS SECTION UPDATED WITH FINAL PLANNED ALGORITHM *****************;

***** Create a variable for the AHRQ CCS acute diagnosis based exclusions for planned ****;
***** Some diagnosis groups are split by ICD-9 diagnosis codes                        ****;
** added on 11/2 Version 2.1: 
 CCS 129 to acute list, CCS 224 and 170 to planned list , remove diagnosis codes 410.x2
 from acute list CCS 100

REVISED: ADD a split for Biliary tract disease  9/2013  add Acute Pancreatitis and HTN w/ Comp
******************************************************************************************;

if ADDXG_p in ('1','2','3','4','5','7','8','9','54','55','60','61','63','76','77','78','82'
,'83','84','85','87','89','90','91','92','93', '102','104','107','109','112',
'116','118','120','122','123','99',
'124','125','126','127','128','129','130','131','135',
'137','139','140','142','145','146','148',
'153','154','157','159','165','168','172','197','198','225','226','227','228','229','230',
'232','233','234','235','237','238','239','240','241','242','243','244','245','246','247',
'249','250','251','252','253','259','650','651','652','653','656','658','660','661','662','663','670')
OR
( addxg_p in ('105','106') and  diag1
 in ('4260','42610','42611','42612','42613','4262',
'4263','4264','42650','42651','42652','42653','42654','4266','4267','42681','42682',
'4269','4272','7850','42789','4279','42769') )
OR
(addxg_p in ('97') and  diag1 in 
('03282','03640','03641','03642','03643','07420','07421','07422','07423',
'11281','11503','11504','11513','11514','11593','11594',
'1303','3910','3911','3912','3918','3919','3920','3980',
'39890','39899','4200','42090','42091','42099','4210','4211',
'4219','4220','42290','42291','42292','42293','42299','4230',
'4231','4232','4233','4290'))
OR
(addxg_p in ('108') and  diag1 in 
('39891','4280','4281','42820','42821','42823','42830','42831',
'42833','42840','42841','42843','4289')) 
OR
( addxg_p in ('100') and  (DIAG1=:'410' AND SUBSTR(DIAG1, 5, 1)^='2'))
OR
( addxg_p in ('149') and diag1 in ('5740','57400','57401','5743','57430','57431',
 '5746','57460','57461','5748','57480','57481','5750','57512','5761')) 
OR
( addxg_p in ('152') and diag1 in ('5770')) 
then excldx = 1; else excldx = 0;



*('393''4238','4239')   remove from acute list of 97 diagnosis to match tracker 12/27/12;  

ARRAY PROCCS(25) $  PROCCCP_1 - PROCCCP_25;
planned_1 = 0; planned_2=0;

****CREATE ALWAYS PLANNED PROCEDURE VARIABLE*******;
DO I=1 TO 25;
		IF proccs(I) IN 
('64','105','176','134','135')THEN do;     
   proc_2  = proccs(I);
   planned_2 = 1; 
   end;
end;

***Determine if Planned Procedure Occurred:  REVISED SEP 2013
REMOVE 211 and 224 per valdiation results  ****; 
DO I=1 TO 25;
		IF proccs(I) IN 
('3','5','9','10','12','33','36','38','40','43','44','45','47','48','49',
'51','52','53','55','56','59','62','66','67','74','78','79','84',
'85','86','99','104','106','107','109','112','113','114','119','120',
'124','129','132','142','152','153','154','157','158','159',
'166','167','172','170') THEN do;   /*remove 169 for Stroke    CW 04-04-2014*/
   procnum  = proccs(I);
   planned_1 = 1; 
   end;
end;
**********ADD ICD_9_CM Proc code level Planned Procedures *****;
ARRAY pproc(25) $   PROC1 -  PROC25;
DO J=1 TO 25;
if  pproc(J) in ('9426','9427') then do;
procnum  = '990';
planned_1 = 1; 
end;
if  pproc(J) in ('304','3174','346','301','3029','303') then do;
procnum  = '991';
planned_1 = 1; 
end;
if  pproc(J) in ('5503','5504') then do;
procnum  = '992';
planned_1 = 1; 
end;
if  pproc(J) in ('3818') then do;
procnum  = '993';
planned_1 = 1; 
end;
END;

planned = 0;

/*step1: Always Planned Procedures*/
if planned_2 = 1 then do; planned = 1; procnum = proc_2;
end;

/*step2: Always Planned Diagnoses*/ ****** Maintenance Chemo Therapy  ******;  ****** Rehabilitation Therapy  ******;
else if ADDXG_p = '45' then do;	
planned = 1;   procnum = '999' ;                        
end;
else if ADDXG_p = '254' then do;
planned = 1;    procnum = '998' ;                      
end;
else if ADDXG_p = '194' then do;
planned = 1;   procnum = '997' ;                        
end;
else if ADDXG_p = '196' then do;
planned = 1;   procnum = '996' ;                      
end;
 ****** Forcep Delivery  ******;   ****** Normal Delivery  ******;
/*step3: All Other Planned */
else if planned_1 =1 and excldx = 1 then planned = 0;
else if planned_1 =1  and excldx = 0 then planned = 1;
run;

 
PROC SORT DATA=POST out=post;
	BY HICNO CASE ADMIT;
RUN;

proc freq data=post;
	tables year;
	where factype="IRF";
run;


data readm1 QA_DupIndex; 
MERGE _ALL (IN=A)
	 post (IN=B /*where=(va_obs_stay=0)*/ KEEP=HICNO ADMIT DISCH PROVID year DIAG1 CASE planned 
			factype sslssnf pmt_amt tot_chg DRGCD DDEST 
			RENAME=(DIAG1=_DIAG1 ADMIT=_ADMIT DISCH=_DISCH PROVID=_PROVID CASE=CASE_P factype=_factype
					sslssnf=_sslssnf pmt_amt=_pmt_amt tot_chg=_tot_chg DRGCD=_DRGCD DDEST=_DDEST year=_year));
BY HICNO CASE_P;

IF A;

/* RADM&loe.ALL: ANY READMISSION WITHIN &loe. DAYS */

IF NOT B THEN RADM&loe.=0;
ELSE IF 0 <= _ADMIT - DISCH <=&loe. & _factype ~in("IRF","SNF","LTC","HHA","OUT","MDS") then RADM&loe.=1;
ELSE IF _ADMIT - DISCH > &loe. then RADM&loe.=0;

IF NOT B THEN RADM45=0;
ELSE IF 0 <= _ADMIT - DISCH <=45 & _factype ~in("IRF","SNF","LTC","HHA","OUT","MDS") then RADM45=1;
ELSE IF _ADMIT - DISCH > 45 then RADM45=0;

INTERVAL=_ADMIT - DISCH;
SAME=(PROVID=_PROVID); /* SAME HOSPITAL READMISSION */

radm&loe.p=0;
if planned =1 and Radm&loe. =1 then do;
	Radm&loe.  = 0;
	radm&loe.p = 1;
	radm45=0;
	radm45p=1;
end;
   
IF _DRGCD IN (61,62,63,64,65,66) & _factype ~in("IRF","SNF","LTC","HHA","OUT","MDS") THEN RADM_SK=1;
ELSE RADM_SK=0;  
/* any readmission with principal diagnosis eq V57 is not counted as readmission,
	added 1/11/2010. ZQ */

if upcase(_diag1)=:'V57' then Radm_rehab=1;
else Radm_rehab=0;

/* any readmission with psych principal diagnosis eq in range of 290-319 that was 
	within 1 day of the discharge date of index admission with discharge dispostion
	eq 65 is not counted as readmission, added 1/11/2010. ZQ */ 

if (_diag1=:'29' or _diag1=:'30' or _diag1=:'31') and (interval in (0,1)) and
	ddest=65 & _factype ~in("IRF","SNF","LTC","HHA","OUT","MDS") then radm_psy=1;
else Radm_psy=0;

****** These psych and rehab stays will not be counted as planned or unplanned readmissions 12/19/12***;

if radm_rehab=1 and (radm&loe.=1 or radm&loe.p = 1) then do;  
  radm&loe.=0; radm&loe.p = 0; /*Removed setting interval to 999. BR 5-8-17*/
end;
if radm_psy=1 and (radm&loe.=1 or radm&loe.p = 1) then do;  
  radm&loe.=0; radm&loe.p = 0; /*Removed setting interval to 999. BR 5-8-17*/
end;
 
hicno_case=strip(hicno)||strip(case_p);

/* PART OF TRANS BUNDLE, SHOULD HAVE BEEN EXCLUDED */
IF RADM&loe.=1 AND RADM_SK=1 AND INTERVAL=0 AND SAME=1 THEN TRANS_BUNDLE=1;
ELSE TRANS_BUNDLE=0; 

IF TRANS_BUNDLE=1 THEN SAMPLE=0;

IF ADMIT=_ADMIT AND DISCH=_DISCH AND PROVID=_PROVID AND DIAG1=_DIAG1 THEN OUTPUT QA_DupIndex;
ELSE OUTPUT readm1;

run;

proc freq data=readm1;
	tables year;
	where _factype="IRF";
run;
 

*****************************************************************************************************************; 
***** Sort Order has changed so that the first readmission is the only one counted either as planned or unplanned**;
***** November 2012   ***************;
*****************************************************************************************************************; 

proc sort data=readm1;
by hicno_case interval; 
run;


data readm1data;
set readm1;
if interval <=180;
/*by hicno_case;
if first.hicno_case;
DROP HICNO_CASE;*/
run;

proc sort data=readm1data;
by hicno case;
run;

DATA sample;
set readm1data (where=(sample=1));
csex=put(sex, $1.);
drop sex;
RUN;

proc sort data=sample out=R.sample_&CONDITION.;
by hicno_case;
run;

proc freq data=R.sample_&CONDITION.;
	tables year _factype _factype*year;
run;

data test_disch;
	set R.sample_&CONDITION.;
	miss_snf_disch=(_disch=. & _admit~=. & _factype="SNF");
	miss_mds_disch=(_disch=. & _admit~=. & _factype="MDS");
run;

title "Check missing SNF and MDS discharges when there is an admission";
proc freq data=test_disch;
	tables year*(miss_snf_disch miss_mds_disch);
run;
title;


/****************************************************************************************************
 Add in the cost-to-charge ratios for the index stay and PAC facilities (SNF, IRF, LTC, and STA). 
 These are at the provider-year level
****************************************************************************************************/

data ach_ccr;
	set ccr.ach_ccr_all_trim (keep=prov_id cost_yr cal_total_ccr cal_total_ccr_c rename=(prov_id=_provid cost_yr=_year));
	if _year<2010 then do;
		_ccr=cal_total_ccr;
		if cal_total_ccr=. then _ccr=cal_total_ccr_c;
	end;
	if _year>=2010 then do;
		_ccr=cal_total_ccr_c;
		if cal_total_ccr_c=. then _ccr=cal_total_ccr;
	end;
	_factype="STA";
run;

data irf_ccr;
	set ccr.irf_ccr_all_trim (keep=prov_id cost_yr cal_total_ccr cal_total_ccr_c rename=(prov_id=_provid cost_yr=_year));
	if _year<2010 then do;
		_ccr=cal_total_ccr;
		if cal_total_ccr=. then _ccr=cal_total_ccr_c;
	end;
	if _year>=2010 then do;
		_ccr=cal_total_ccr_c;
		if cal_total_ccr_c=. then _ccr=cal_total_ccr;
	end;
	_factype="IRF";
run;

data ltc_ccr;
	set ccr.ltch_ccr_all_trim (keep=prov_id cost_yr cal_total_ccr cal_total_ccr_c rename=(prov_id=_provid cost_yr=_year));
	if _year<2010 then do;
		_ccr=cal_total_ccr;
		if cal_total_ccr=. then _ccr=cal_total_ccr_c;
	end;
	if _year>=2010 then do;
		_ccr=cal_total_ccr_c;
		if cal_total_ccr_c=. then _ccr=cal_total_ccr;
	end;
	_factype="LTC";
run;
		
/*Same for SNF*/
data snf_ccr;
	set ccr.snf_ccr_all_trim (keep=prov_id cost_yr cal_total_ccr rename=(prov_id=_provid cost_yr=_year cal_total_ccr=_ccr));
	_factype="SNF";
run;

/*HHA is continuous from 1996-2014. Uses CPV not CCR*/
data hha_cpv;
	set ccr.hha_cpv_calyear_trim (keep=prov_id calyear calyear_total_cpv rename=(prov_id=_provid calyear=_year calyear_total_cpv=_cpv));
	_factype="HHA";
run;

/*Create flags for facilities that report CCR or CPV in all years of interest*/
proc sort data=ach_ccr nodupkey out=ach_allyr; by _provid _year _factype; run;
proc sort data=irf_ccr nodupkey out=irf_allyr; by _provid _year _factype; run;
proc sort data=ltc_ccr nodupkey out=ltc_allyr; by _provid _year _factype; run;
proc sort data=snf_ccr nodupkey out=snf_allyr; by _provid _year _factype; run;
proc sort data=hha_cpv nodupkey out=hha_allyr; by _provid _year _factype; run;

/*Output a list of unique providers who report a cost multiplier for each year of our data*/
%macro ccr_allyr(fac,ccr);
	data &fac._allyr (keep=_provid _factype);
		set &fac._allyr (where=(_year>=20&YY. & _year<=20&YYE.)); /*Keep the years we are interested in*/
		retain totyr; /*Count how many years each provider has non-missing cost multipliers*/
		by _provid;
		if first._provid then totyr=0;
		if _&ccr.~=. then totyr+1;
		if last._provid & totyr=%eval(&YYE.-&YY.)+1 then output; /*Ex. if 2003-2013 we want 11 consecutive yearly CCRs for a provider*/
	run;
%mend;
%ccr_allyr(ach,ccr);
%ccr_allyr(irf,ccr);
%ccr_allyr(ltc,ccr);
%ccr_allyr(snf,ccr);
%ccr_allyr(hha,cpv);

/*Merge on PAC CCRs*/
proc sort data=ach_ccr nodupkey; by _provid _year _factype; run;
proc sort data=irf_ccr nodupkey; by _provid _year _factype; run;
proc sort data=ltc_ccr nodupkey; by _provid _year _factype; run;
proc sort data=snf_ccr nodupkey; by _provid _year _factype; run;
proc sort data=hha_cpv nodupkey; by _provid _year _factype; run;
proc sort data=R.sample_&condition.; by _provid _year _factype; run;

data R.sample_&condition.;
	merge R.sample_&condition. (in=a)
		  ach_ccr (in=b)
		  irf_ccr (in=c)
		  ltc_ccr (in=d)
		  snf_ccr (in=e)
		  hha_cpv (in=f);
	by _provid _year _factype;
	if a;
run;

/*Merge on the flags indicating provider has CCRs in all years*/
proc sort data=R.sample_&condition.; by _provid _factype; run;

data R.sample_&condition.;
	merge R.sample_&condition. (in=a)
		  ach_allyr (in=b)
		  irf_allyr (in=c)
		  ltc_allyr (in=d)
		  snf_allyr (in=e)
		  hha_allyr (in=f);
	by _provid _factype; /*Must merge on by provider ID and factype because some IRFs have same ID as ACH*/
	if a;
	if b | c | d | e | f then _cost_allyr=1;
run;

/*Merge on Index Stay CCRs*/
data index_ccr;
	set ccr.ach_ccr_all_trim (keep=prov_id cost_yr cal_total_ccr cal_total_ccr_c rename=(prov_id=provid));
	if cost_yr<2010 then do;
		index_ccr=cal_total_ccr;
		if cal_total_ccr=. then index_ccr=cal_total_ccr_c;
	end;
	if cost_yr>=2010 then do;
		index_ccr=cal_total_ccr_c;
		if cal_total_ccr_c=. then index_ccr=cal_total_ccr;
	end;
	year=put(cost_yr,4.); /*Make year character to keep it consistent with the main dataset*/
run;

proc sort data=index_ccr nodupkey; by provid year; run;

/*Output a list of unique providers who report a cost multiplier for each year of our data*/
proc sort data=index_ccr nodupkey out=index_allyr; by provid year; run;

data index_allyr (keep=provid);
	set index_allyr (where=(cost_yr>=20&YY. & cost_yr<=20&YYE.)); /*Keep the years we are interested in*/
	retain totyr; /*Count how many years each provider has non-missing cost multipliers*/
	by provid;
	if first.provid then totyr=0;
	if index_ccr~=. then totyr+1;
	if last.provid & totyr=%eval(&YYE.-&YY.)+1 then output; /*Ex. if 2003-2013 we want 11 consecutive yearly CCRs for a provider*/
run;

proc sort data=R.sample_&condition.; by provid year; run;

data R.sample_&condition.;
	merge R.sample_&condition. (in=a)
		  index_ccr (in=b);
	by provid year;
	if a;
run;

/*Merge on the flags indicating provider has CCRs in all years*/
data R.sample_&condition.;	
	merge R.sample_&condition. (in=a)
		  index_allyr (in=b);
	by provid;
	if a;
	if b then cost_allyr=1;
run;

title "Check providers that have a CCR or CPV in all years of data";
proc freq data=R.sample_&condition.;
	tables cost_allyr _factype*_cost_allyr / missing;
run;
title;

/*Sort by hicno-case again*/
proc sort data=R.sample_&CONDITION.;
	by hicno_case;
run;


/***************************************************************************************************************** 
FLATTEN THE FILE OUT SO EACH BENE/CASE IS ITS OWN OBSERVATION, AND ALL SUBSEQUENT CARE DATES ARE STORED AS VARS
*****************************************************************************************************************/
/*Transpose to make a single observation for each index admission that has all utilizations within 30 days of discharge*/
%macro factype(var);

	data sample_&condition. (keep=hicno case hicno_case case_count _factype radm30 radm45 dd30
								  ADMIT DISCH PROVID PROC1 pmt_amt tot_chg DRGCD DDEST sslssnf
								  _ADMIT _DISCH _PROVID _PROC1 _pmt_amt _tot_chg _DRGCD _DDEST 
								  _sslssnf DEATH year DIAG1-DIAG25 age birth male rti_race_cd race
								  sex index_ccr _ccr _cpv cost_allyr _cost_allyr);
		set R.sample_&CONDITION.;
		retain case_count;
		by hicno_case;
		if first.hicno_case then case_count=0;
		if upcase(_factype)="&var." then case_count+1;
	run;

	proc means data=sample_&condition. noprint missing;
	 var case_count;
	 output out=max_obs (drop=_FREQ_ _TYPE_)
	 max=max_case_count;
	run;

	data _null_;
	 set max_obs;
	 call symput ('N',Trim(Left(max_case_count)));
	run; 

	data sample_&condition._&var. (drop=radm30 radm45 dd30 
								   %if "&var."="STA" %then %do;
								   	rename=(rradm30=radm30 rradm45=radm45 rdd30=dd30) /*Readmissions only count from STA facilities*/
								   %end;
								   %else %do;
								   	rename=(rdd30=dd30) /*Deaths count from all facilities*/
								   %end;
								   );
		set sample_&condition. (where=(_factype="&var."));
		by hicno_case;


			array &var.adm{&N.}   &var._admit1-&var._admit&N.;
			array &var.dis{&N.}   &var._disch1-&var._disch&N.;
			array &var.pmt{&N.}   &var._pmt_amt1-&var._pmt_amt&N.;
			array &var.tot{&N.}   &var._tot_chg1-&var._tot_chg&N.;
			array &var.ccr{&N.}   &var._ccr1-&var._ccr&N.;
			array &var.all{&N.}	  &var._cost_allyr1-&var._cost_allyr&N.;
			array &var.cpv{&N.}   &var._cpv1-&var._cpv&N.;
			array &var.drg{&N.} $ &var._drg1-&var._drg&N.;
			array &var.dst{&N.} $ &var._dstntncd1-&var._dstntncd&N.;
			array &var.dgn{&N.} $ &var._proc1-&var._proc&N.;
			array &var.prv{&N.} $ &var._provid1-&var._provid&N.;
			array &var.ssl{&N.} $ &var._sslssnf1-&var._sslssnf&N.;

		if first.hicno_case then do;
			i=1;
			/*Only the first hospitalization counts towards a readmission*/
			%if "&var."="STA" %then %do;
				rradm30=radm30;
				rradm45=radm45;
			%end;
				rdd30=0;
		end; 
		else i+1;


			if upcase(_factype)="&var." then do;
				&var.adm{i}=_ADMIT;
				&var.dis{i}=_DISCH;
				&var.pmt{i}=_pmt_amt;
				&var.tot{i}=_tot_chg;
				&var.ccr{i}=_ccr;
				&var.all{i}=_cost_allyr;
				&var.drg{i}=_DRGCD;
				&var.dst{i}=_DDEST;
				&var.dgn{i}=_PROC1;
				&var.prv{i}=_PROVID;
				&var.ssl{i}=_sslssnf;

				if "&var."="HHA" then do; /*cost-per-visit is the HHA equivalent of cost-to-charge ratio for the other facilities*/
					&var.cpv{i}=_cpv;
				end;
				
			end;

		*if radm30=1 then rradm30=1;
		*if radm45=1 then rradm45=1;
		if dd30=1 then rdd30=1;

		if last.hicno_case;
		if i lt &N then do i=i+1 to &N;
				&var.adm{i}=.;
				&var.dis{i}=.;
				&var.pmt{i}=.;
				&var.tot{i}=.;
				&var.ccr{i}=.;
				&var.all{i}=.;
				&var.drg{i}='';
				&var.dst{i}='';
				&var.dgn{i}='';
				&var.prv{i}='';
				&var.ssl{i}='';

				if "&var."="HHA" then do; /*Don't create CPV variables for non-HHA facilities*/
					&var.cpv{i}=.;
				end;
		end;

		retain  &var._admit1-&var._admit&N.
				&var._disch1-&var._disch&N.
				&var._pmt_amt1-&var._pmt_amt&N.
				&var._tot_chg1-&var._tot_chg&N.
				&var._ccr1-&var._ccr&N.
				&var._cost_allyr1-&var._cost_allyr&N.
				&var._drg1-&var._drg&N.
				&var._dstntncd1-&var._dstntncd&N.
				&var._proc1-&var._proc&N.
				&var._provid1-&var._provid&N.
				&var._sslssnf1-&var._sslssnf&N.
				%if "&var."="HHA" %then %do;
					&var._cpv1-&var._cpv&N.
				%end;
				rradm30 rradm45 rdd30;
	run;

	proc print data=sample_&condition._&var. (obs=10); run;

	proc sort data=sample_&condition._&var.; by hicno_case; run;
%mend;
%factype(STA);

proc freq data=sample_&condition._STA;
	tables radm30 radm45;
run;

%factype(SNF);
%factype(IRF);
%factype(LTC);
%factype(HHA);
%factype(MDS);
%factype(); /*Blank macro parameter to account for beneficiaries with no PAC or readmission*/
*%factype(OUT);

data R.collapsed_&CONDITION.;
	merge sample_&condition._:;
	by hicno_case;
	/*Set missing values for these variables to 0*/
	radm30=(radm30=1);
	radm45=(radm45=1);
	dd30=(dd30=1);
run;

proc print data=R.sample_&CONDITION. (obs=100);
	var hicno case hicno_case ADMIT DISCH PROVID pmt_amt tot_chg DRGCD DDEST sslssnf
		_ADMIT _DISCH _PROVID _pmt_amt _tot_chg _DRGCD _DDEST _sslssnf factype _factype;
run;
proc print data=R.collapsed_&CONDITION. (obs=100);
	var hicno_case admit disch provid pmt_amt tot_chg drgcd ddest sslssnf
		sta_admit1-sta_admit3 sta_disch1-sta_disch3 sta_pmt_amt1-sta_pmt_amt3
		snf_admit1-snf_admit3 snf_disch1-snf_disch3 snf_pmt_amt1-snf_pmt_amt3
		irf_admit1-irf_admit3 irf_disch1-irf_disch3 irf_pmt_amt1-irf_pmt_amt3
		ltc_admit1-ltc_admit2 ltc_disch1-ltc_disch2 ltc_pmt_amt1-ltc_pmt_amt2
		hha_admit1-hha_admit3 hha_disch1-hha_disch3 hha_pmt_amt1-hha_pmt_amt3
		mds_admit1-mds_admit3 mds_disch1-mds_disch3 mds_pmt_amt1-mds_pmt_amt3
		/*out_admit1-out_admit3 out_disch1-out_disch3 out_pmt_amt1-out_pmt_amt3*/;
	format sta_admit1-sta_admit3 sta_disch1-sta_disch3 snf_admit1-snf_admit3 snf_disch1-snf_disch3
		   irf_admit1-irf_admit3 irf_disch1-irf_disch3 ltc_admit1-ltc_admit2 ltc_disch1-ltc_disch2
		   hha_admit1-hha_admit3 hha_disch1-hha_disch3 /*out_admit1-out_admit3 out_disch1-out_disch3*/
		   mds_admit1-mds_admit3 mds_disch1-mds_disch3
		   DATE9.;
run;

proc print data=R.collapsed_&CONDITION. (obs=100);
	var hicno_case admit disch provid pmt_amt tot_chg drgcd ddest sslssnf
		sta_admit1-sta_admit3 sta_disch1-sta_disch3 sta_pmt_amt1-sta_pmt_amt3
		snf_admit1-snf_admit3 snf_disch1-snf_disch3 snf_pmt_amt1-snf_pmt_amt3
		irf_admit1-irf_admit3 irf_disch1-irf_disch3 irf_pmt_amt1-irf_pmt_amt3
		ltc_admit1-ltc_admit2 ltc_disch1-ltc_disch2 ltc_pmt_amt1-ltc_pmt_amt2
		hha_admit1-hha_admit3 hha_disch1-hha_disch3 hha_pmt_amt1-hha_pmt_amt3
		mds_admit1-mds_admit3 mds_disch1-mds_disch3 mds_pmt_amt1-mds_pmt_amt3
		/*out_admit1-out_admit3 out_disch1-out_disch3 out_pmt_amt1-out_pmt_amt3*/;

	where mds_admit1~=.;

	format sta_admit1-sta_admit3 sta_disch1-sta_disch3 snf_admit1-snf_admit3 snf_disch1-snf_disch3
		   irf_admit1-irf_admit3 irf_disch1-irf_disch3 ltc_admit1-ltc_admit2 ltc_disch1-ltc_disch2
		   hha_admit1-hha_admit3 hha_disch1-hha_disch3 /*out_admit1-out_admit3 out_disch1-out_disch3*/
		   mds_admit1-mds_admit3 mds_disch1-mds_disch3
		   DATE9.;
run;

proc freq data=R.collapsed_&CONDITION.;
	tables radm30 radm45 dd30 radm30*_factype;
run;

proc means data=R.collapsed_&CONDITION.;
	var index_ccr SNF_ccr1 IRF_ccr1 LTC_ccr1 STA_ccr1 HHA_cpv1;
	where year<='2008';
run; 




/*******************************************************************************************************
********************************************************************************************************
BELOW IS A REPORTING TOOL FOR THE SUMMARY. IT RETURNS N's FROM THE INPUTS, OUTPUTS, AND KEY INTERMEDIATE 
STEPS
********************************************************************************************************
*******************************************************************************************************/

%macro nobs(data,name); /*Testing the observation counting macro. BR 5-11-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=RAW.POSTINDEX_&CONDITION._&YEAR, name=obs);
%nobs(data=RAW.READMISSIONS_INDEX_&CONDITION._&YEAR, name=obs); 

/*Test out the how many of our specific exclusions get dropped*/
/*Starting number after dropping duplicates and combining overlapping claims from READMISSIONS_INDEX_SK */
%nobs(data=TEMP3, name=obs);

/*How many &CONDITION. cases out of the original N in TEMP3*/
proc freq data=all;
	tables &CONDITION._case / out=count;
run;
proc print data=count; run;
proc contents data=count; run;
data _null_;
	set count;
	if trim(left(&CONDITION._case))=1 then call symput("&CONDITION._case",trim(left(COUNT)));
run;

/*How many age 65+ cases out of the TEMP3 with &CONDITION._case*/
proc freq data=all;
	tables age65 / out=count;
	where &CONDITION._case=1;
run;
data _null_;
	set count;
	if trim(left(age65))=1 then call symput("age65",trim(left(COUNT)));
run;

/*How many with 12 months consecutive FFS coverage prior to admission, out of the TEMP3 with &CONDITION._case and Age65*/
proc freq data=all;
	tables PRIOR12 / out=count;
	where &CONDITION._case=1 & age65=1;
run;
data _null_;
	set count;
	if trim(left(PRIOR12))=1 then call symput("PRIOR12",trim(left(COUNT)));
run;

/*How many have continuous post-discharge FFS coverage, among those with 12 months consecutive FFS coverage prior to admission, with &CONDITION._case, and Age65*/
proc freq data=all;
	tables POST1 / out=count;
	where &CONDITION._case=1 & age65=1 & PRIOR12=1;
run;
data _null_;
	set count;
	if trim(left(POST1))=1 then call symput("POST1",trim(left(COUNT)));
run;

/*How many are transfer eligible among those with pre and post coverage, &CONDITION._case, and age65*/
proc freq data=all;
	tables TRANS_COMBINE / out=count;
	where &CONDITION._case=1 & age65=1 & PRIOR12=1 & POST1=1;
run;
data _null_;
	set count;
	if trim(left(TRANS_COMBINE))=0 then call symput("TRANS_COMBINE",trim(left(COUNT)));
run;

/*How many not released AMA out of those non-transfers, with pre and post coverage, &CONDITION._case, and age65*/
proc freq data=all;
	tables AMA / out=count;
	where &CONDITION._case=1 & age65=1 & PRIOR12=1 & POST1=1 & TRANS_COMBINE=0;
run;
data _null_;
	set count;
	if trim(left(AMA))=0 then call symput("AMA",trim(left(COUNT)));
run;

/*How many total are eligible, including those not released AMA, non-transfers, with pre and post coverage, &CONDITION._case, and age65*/
proc freq data=all;
	tables criteria / out=count;
	where &CONDITION._case=1 & age65=1 & PRIOR12=1 & POST1=1 & TRANS_COMBINE=0 & AMA=0;
run;
data _null_;
	set count;
	if trim(left(criteria))=1 then call symput("criteria",trim(left(COUNT)));
run;

%put Summary Report - Out of benes in TEMP3, there were &&&CONDITION._case. benes kept that met the DX criteria.;
%put Summary Report - Out of these, &age65. benes were 65 or older.;
%put Summary Report - Out of these, &prior12. benes had 12 months continuous FFS coverage before admission.;
%put Summary Report - Out of these, &post1. benes had 6 months continuous FFS coverage after discharge (or died).;
%put Summary Report - Out of these, &trans_combine. benes were non-transfers, or were at the end of a transfer chain.;
%put Summary Report - Out of these, &AMA. benes were not discharged against medical advice.;
%put Summary Report - In total, &criteria. benes were left in the sample after inclusion criteria.;
%put Summary Report - Below in INDEXADMISSION is the total benes after dropping out index admissions within 30 days of each other:;

/*After dropping index stays within 30 days of each other*/
%nobs(data=INDEXADMISSION, name=obs);
%nobs(data=R.collapsed_&CONDITION., name=obs); 
