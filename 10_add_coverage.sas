/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 3_add_coverage_and_hospice

FOR USE WITH: ALL MEASURES

PURPOSE:      Calculate completeness fields and hospice fields 

OVERVIEW:     Check each hospice occurence tiemframe for meeting criteria fir hospice flags.
			  Calculate each of the completeness fields using formatted coverage data. label
			  index fields.

INPUT DATA: 
	dx_data.index02             *PRE-SORTED BY HICNO CASE
	data_an.coverage&YYE._&MM.  *PRE-SORTED BY HICNO

OUTPUT FILES: 
	dx_data.index03
	dx_data.missing_coverage

UPDATES:
	Updated to include 2012 data. Look into automating this process. BR 9/23/15.
	Updated to include 2013 data. BR 10/30/15.
	Updated to work for HRRP project. BR 11/24/15.
	Updated to autogenerate code based on dates instead of requiring hardcoding. BR 11/24/15.

************************************************************************************************************/

OPTIONS COMPRESS=YES mprint;

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
PUT "  Job Name: 3_add_coverage_and_hospice" /;
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
%remove(ds=dx_data.index03);
/**********************************************************************************************
**********************************************************************************************/

%let n_yr = %sysevalf(&YYE.-&YY.);
%put &n_yr.;

%macro setup_cov;
	%do n=1 %to &n_yr.;
		%global YP&n.;
		%let YP&n.=%eval(&YYE.-&n.);
		%if &&YP&n.<10 %then %let YP&n.=0&&YP&n.;
	%end;

	%put &YP1. &YP4.;
%mend;
%setup_cov;

/*DEFINE MACRO VARIABLE FOR NUMBER OF MONTHS FOR COVERAGE FIELDS*/
DATA _NULL_;
	frstday = MDY(1,1,20&&YP&n_yr.); 
	lstmonth = MDY(&MM,1,20&YYE.); 
	mthCt = INTCK('MONTH',frstday,lstmonth)+1;
	CALL SYMPUT('mthCt',COMPRESS(mthCt));
RUN;

proc sort data=data_an.coverage&YYE._&MM.; by hicno; run;

********************************************************************
* CALCULATE COMPLETENESS FIELDS AND HOSPICE FIELDS 
**********************************************************************;
DATA index03
	 dx_data.missing_coverage;
	MERGE dx_data.index02(IN=INI) data_an.coverage&YYE._&MM.(IN=COV /*DROP=hspc_ct*/);
	BY hicno;
	IF INI;

	LENGTH para para_b postmo postmo_a postmod postmod_a premo premo_a 
	       admmo dismo begin begind post postd 3;

	ARRAY BUY(&mthCt) $ %macro buy_array;
							%do b=&n_yr. %to 1 %by -1;
								Y&&YP&b..buy1-Y&&YP&b..BUY12
							%end;
							Y&YYE.buy1-Y&YYE.BUY&MM.;
						%mend;
						%buy_array; 

ARRAY HMO(&mthCt) $ %macro hmo_array;
							%do b=&n_yr. %to 1 %by -1;
								Y&&YP&b..hmo1-Y&&YP&b..HMO12
							%end;
							Y&YYE.hmo1-Y&YYE.HMO&MM.;
						%mend;
						%hmo_array; 
	/*DEFINE STARTING POINT FOR COUNTING MONTHS*/
	frstday = MDY(1,1,20&&YP&n_yr.); 

	/***********************************************************************
	 MACRO'D OUT CODE. KEEP FOR REFERENCE BECAUSE IT IS HIGHLY READABLE
	***********************************************************************/
							%macro out;
							/*This is what the above code automates. They should be identical, but the above does not need updating*/
							**********************************************************************
							* CALCULATE AND VERIFY MACRO VARIABLES
							**********************************************************************;
							/*CALCULATE PRIOR YEARS*/
							%LET YP1=%EVAL(&YYE.-1);        *PRECEDING YEAR;
							%LET YP2=%EVAL(&YYE.-2);        *2 YEARS PRIOR TO CURRENT YEAR;
							%LET YP3=%EVAL(&YYE.-3);        *3 YEARS PRIOR TO CURRENT YEAR;
							%LET YP4=%EVAL(&YYE.-4);        *4 YEARS PRIOR TO CURRENT YEAR;
							%LET YP5=%EVAL(&YYE.-5);        *5 YEARS PRIOR TO CURRENT YEAR;
							%LET YP6=%EVAL(&YYE.-6);        *6 YEARS PRIOR TO CURRENT YEAR;
							%LET YP7=%EVAL(&YYE.-7);        *7 YEARS PRIOR TO CURRENT YEAR;
							%LET YP8=%EVAL(&YYE.-8);        *8 YEARS PRIOR TO CURRENT YEAR;
							%LET YP9=%EVAL(&YYE.-9);        *9 YEARS PRIOR TO CURRENT YEAR;
							%LET YP10=%EVAL(&YYE.-10);      *10 YEARS PRIOR TO CURRENT YEAR;
							%LET YP11=%EVAL(&YYE.-11);      *11 YEARS PRIOR TO CURRENT YEAR;


							/*ZERO PAD YEARS LESS THAN 10*/
							%MACRO ZEROYEAR();
								%IF &YP1<10 %THEN %LET YP1=0&YP1;
								%IF &YP2<10 %THEN %LET YP2=0&YP2;
								%IF &YP3<10 %THEN %LET YP3=0&YP3;
								%IF &YP4<10 %THEN %LET YP4=0&YP4;
								%IF &YP5<10 %THEN %LET YP5=0&YP5;
								%IF &YP6<10 %THEN %LET YP6=0&YP6;
								%IF &YP7<10 %THEN %LET YP7=0&YP7;
								%IF &YP8<10 %THEN %LET YP8=0&YP8;
								%IF &YP9<10 %THEN %LET YP9=0&YP9;
								%IF &YP10<10 %THEN %LET YP10=0&YP10;
								%IF &YP11<10 %THEN %LET YP10=0&YP11;
							%MEND;
							%ZEROYEAR();

							/*DEFINE MACRO VARIABLE FOR NUMBER OF MONTHS FOR COVERAGE FIELDS*/
							DATA _NULL_;
								frstday = MDY(1,1,20&YP10.); 
								lstmonth = MDY(&MM,1,20&YYE.); 
								mthCt = INTCK('MONTH',frstday,lstmonth)+1;
								CALL SYMPUT('mthCt',COMPRESS(mthCt));
							RUN;

							/*OUTPUT YEAR AND END MONTH MACRO VARIABLES TO LOG*/
							%PUT YEAR AND MONTH MACRO VARIABLES:;
							%PUT YYE=&YYE YP1=&YP1 YP2=&YP2 YP3=&YP3 YP4=&YP4 YP5=&YP5 MM=&MM mthCt=&mthCt;
							%PUT MAX NUMBER OF DIAGNOSIS AND PROCEDURE CODES:;
							%PUT DXN=&DXN PRN=&PRN;
							RUN;
							**********************************************************************;

							proc sort data=data_an.coverage&YYE._&MM.; by hicno; run;

							********************************************************************
							* CALCULATE COMPLETENESS FIELDS AND HOSPICE FIELDS 
							**********************************************************************;
							DATA index03
								 dx_data.missing_coverage;
								MERGE dx_data.index02(IN=INI) data_an.coverage&YYE._&MM.(IN=COV /*DROP=hspc_ct*/);
								BY hicno;
								IF INI;

								LENGTH para para_b postmo postmo_a postmod postmod_a premo premo_a 
								       admmo dismo begin begind post postd 3;

								ARRAY BUY(&mthCt) $ 
												Y&YP11.buy1-Y&YP11.BUY12
												Y&YP10.buy1-Y&YP10.BUY12
												Y&YP9.buy1-Y&YP9.BUY12
												Y&YP8.buy1-Y&YP8.BUY12
												Y&YP7.buy1-Y&YP7.BUY12
												Y&YP6.buy1-Y&YP6.BUY12
												Y&YP5.buy1-Y&YP5.BUY12
												Y&YP4.buy1-Y&YP4.BUY12
												Y&YP3.buy1-Y&YP3.BUY12
												Y&YP2.buy1-Y&YP2.BUY12
												Y&YP1.buy1-Y&YP1.BUY12
												Y&YYE.buy1-Y&YYE.BUY&MM;
								ARRAY HMO(&mthCt) $
												Y&YP11.hmo1-Y&YP11.HMO12
												Y&YP10.hmo1-Y&YP10.HMO12
												Y&YP9.hmo1-Y&YP9.HMO12
												Y&YP8.hmo1-Y&YP8.HMO12
												Y&YP7.hmo1-Y&YP7.HMO12
												Y&YP6.hmo1-Y&YP6.HMO12 
												Y&YP5.hmo1-Y&YP5.HMO12
												Y&YP4.hmo1-Y&YP4.HMO12
												Y&YP3.hmo1-Y&YP3.HMO12
												Y&YP2.hmo1-Y&YP2.HMO12
												Y&YP1.hmo1-Y&YP1.HMO12
												Y&YYE.hmo1-Y&YYE.HMO&MM;

								/*DEFINE STARTING POINT FOR COUNTING MONTHS*/
								frstday = MDY(1,1,20&YP11.); 
							%mend;



	/***********************************************************************
	 RESUME FUNCTIONAL CODE
	***********************************************************************/
	/*Set BUYIN variables to A or C if they are 1 or 3 - Added 7/14/15 BR*/
	do N=1 to &mthCt;
		if BUY(N)='1' then BUY(N)='A';
		else if BUY(N)='3' then BUY(N)='C';
	end;
%macro hospice;
	*CREATE HOSPICE INDICATORS 
	********************************************************************; 
	/*DEFINE ARRAYS FOR CMS HOSPICE DATA*/
	ARRAY HSPBEG{30} HSPBEG1-HSPBEG30;                                             
	ARRAY HSPEND{30} HSPEND1-HSPEND30;                                             

	/*INITIALIZE CMS HOSPICE INDICATORS */
	hsp_enr_prior_admit=0;                                                         
	hsp_enr_on_admit=0;                                                            
	hsp_enr_prior_disch=0;                                                         
	hsp_enr_on_disch=0;                                                            

	/*ITERATE THROUGH CMS HOSPICE DATA TO FLAG CMS HOSPICE INDICATORS*/
	DO I =1 TO 30;     
	/*ENDED HOSPICE STAY WITHIN 365 DAYS PRIOR TO OR ON ADMIT DATE*/
		IF admit-HSPBEG(I)>0 AND admit-HSPEND(I)<=365 THEN hsp_enr_prior_admit=1;  
	/*IN HOSPICE WHEN ADMITTED*/
		IF HSPEND(I)>=admit>=HSPBEG(I) THEN hsp_enr_on_admit=1; 
	/*IN HOSPICE DURING STAY*/
		IF HSPEND(I)>admit AND HSPBEG(I)<disch THEN hsp_enr_prior_disch=1;      
	/*IN HOSPICE WHEN DISCHARGED */
		IF HSPEND(I)>=disch>=HSPBEG(I) THEN hsp_enr_on_disch=1;                      
	END;
%mend;

	*CALCULATE COMPLETENESS VARIABLES
	********************************************************************; 
	/*COUNT # OF MONTHS FROM FRSTDAY TO ADMIT AND DISCH DATE*/
	admmo = INTCK('MONTH',frstday,admit) + 1; /*NUMBER OF MTHS FROM FIRST MTH*/
	dismo = INTCK('MONTH',frstday,disch) + 1; /*NUMBER OF MTHS FROM FIRST MTH*/

	/*PARA: HMO=0 AND PART A ENROLLMENT FOR MONTH OF ADMIT*/
	para =HMO(admmo)='0' AND BUY(admmo) IN ('A','C');

	/*PARA_B: HMO=0 AND PART A AND B ENROLLMENT FOR MONTH OF ADMIT*/
	para_b =HMO(admmo)='0' AND BUY(admmo) = 'C';



	/*POSTMO: COUNT ONLY MONTHS CONTINUOUSLY ENROLLED BEGINNING WITH THE
	FIRST MONTH AFTER ADMISSION. BELOW, POSTMO IS EQUAL TO THE SUM OF ITSELF
	AND THE HMO/BUY-IN CONDITION ONLY WHEN POSTMO=(I-BEGIN) IS TRUE. IF THE
	LATTER STATEMENT IS NOT TRUE FOR A GIVEN ITERATION OF THE DO-LOOP, THEN
	POSTMO HOLDS ITS CURRENT VALUE. ON THE FIRST ITERATION OF THE DO-LOOP
	POSTMO=(I-BEGIN) IS ALWAYS TRUE SINCE POSTMO IS INITIALIZED TO 0 AND
	I-BEGIN MUST ALSO EQUAL 0. IF THE HMO/BUY-IN CONDITION IS NOT TRUE FOR
	A GIVEN MONTH, THEN POSTMO=(I-BEGIN) IS FALSE FOR ALL SUBSEQUENT MONTHS
	AND THEREFORE POSTMO CAN NEVER INCREASE EVEN IF THE PERSON REENROLLS

	EXAMPLE:
		MONTH OF ADMISSION = 1, BEGIN = 2, POSTD=4,
		ELIGIBILITY IN FEBRUARY = 1
		ELIGIBILITY IN MARCH = 0
		ELIGIBILITY IN APRIL = 1

		FIRST PASS: POSTMO = 0, BEGIN = 2, I = 2. POSTMO=(2-2).
		          POSTMO=(I-BEGIN) => 0=0, STATEMENT IS TRUE AND RETURNS A 1
		          ELIGIBILITY CHECK IS TRUE AND RETURNS A 1
		          POSTMO = 0 + 1*1 = 1

		SECOND PASS: POSTMO = 1, BEGIN =2, I=3. POSTMO=(3-2).
		           POSTMO=(I-BEGIN) => 1=1, STATEMENT IS TRUE AND RETURNS A 1
		           ELIGIBILITY CHECK IS FALSE AND RETURNS A 0
		           POSTMO=1+(1*0)=1
		           (POSTMO REMAINS 1 BECAUSE OF ELIGIBILITY IN FEBRUARY)

		THIRD PASS: POSTMO = 1, BEGIN =2, I=4. POSTMO=(4-2)
		          POSTMO=(I-BEGIN) => 1=2, STATEMENT IS FALSE AND RETURNS A 0
		          ELIGIBILITY CHECK IS TRUE AND RETURNS A 1
		          POSTMO = 1 + 0*1 = 1
		          (POSTMO REMAINS 1 BECAUSE OF ELIGIBILITY IN FEBRUARY)

		FINAL VALUE OF POSTMO IS 1

	THE CODE IS REPEATED BELOW FOR POSTMO AND POSTMOD CREATION FOR ALL YEARS*/

	IF dismo < (&mthCt.-5) THEN DO; /*NOT ENOUGH DATA TO CHECK POSTMO FOR POST INDEX RECORDS*/

		/*POSTMO AND POSTMO_A ENROLLMENT FOR 3 MONTHS FOLLOWING ADMISSION*/
		postmo=0;
		postmo_a=0;
		begin=(admmo+1);
		post =(admmo+6);

		DO I=BEGIN TO POST;
			postmo =SUM(postmo,
					   (postmo=(I-BEGIN))* /*count only if continuously enrld*/
					   (HMO(I)='0' AND BUY(I)='C'));
	        /*SAME CODING AS POSTMO BUT WITH ADDITIONAL 'A' BUYIN CODE*/
			postmo_a =SUM(postmo_a,
						 (postmo_a=(I-BEGIN))*
						 (HMO(I)='0' AND (BUY(I) IN ('A','C'))));
		END;

		/*POSTMOD AND POSTMOD_A ENROLLMENT FOR 3 MONTHS FOLLOWING DISCHARGE*/
	    postmod=0;
	    postmod_a=0;
	    BEGIND=(dismo+1);
	    POSTD =(dismo+6);

	      DO I=BEGIND TO POSTD;
	        postmod =SUM(postmod,
	                     (postmod=(I-BEGIND))* /*count only if continuously enrld*/
	                     (HMO(I)='0' AND BUY(I)='C'));
	        /*SAME CODING AS POSTMOD BUT WITH ADDITIONAL 'A' BUYIN CODE*/
	        postmod_a =SUM(postmod_a,
	                       (postmod_a=(I-BEGIND))*
						   (HMO(I)='0' AND (BUY(I) IN ('A','C'))));
	      END;

		/*CREATE CHECK VARIABLES TO CONFIRM THAT POSTMO AND POSTMOD ARE EQUAL WHEN ADMIT AND */
		/*DISCHARGE ARE IN THE SAME MONTH*/
		IF BEGIN=BEGIND THEN DO;
			flag_unequal   = (postmo NE postmod);
			flag_unequal_a = (postmo_a NE postmod_a);
		END;
	END;

	/*PREMO AND PREMO_A CHECK ENROLLMENT IN PREVIOUS 12 MTHS*/
	premo=0;
	premo_a=0;
	* CHECK FOR ENROLLMENT IN 12 MONTHS PRIOR TO ADMISSION;
	BEGIN=ADMMO-12;

	IF BEGIN>0 THEN DO I=BEGIN TO ADMMO-1;
		premo =SUM(premo,(HMO(I)='0' AND BUY(I)='C'));
		premo_a=SUM(premo_a,(HMO(I)='0' AND (BUY(I) IN ('A','C'))));
	END;

	IF (INI AND COV) THEN OUTPUT index03;
	ELSE OUTPUT dx_data.missing_coverage;
RUN;
**********************************************************************;



********************************************************************
*VALIDATE CREATED FIELDS
********************************************************************; 
TITLE 'COVERAGE FREQS CHECK RANGES';
PROC FREQ DATA=index03;
	TABLES 
	para para_b premo premo_a postmo postmod postmo_a postmod_a postmo postmo_a
	admit*(premo para postmo postmo_a)
	disch*(postmod postmod_a)
	para*premo_a

	/*hsp_enr_prior_admit
	hsp_enr_on_admit
	hsp_enr_prior_disch
	hsp_enr_on_disch*/		/*No hospice data available. BR*/
	/LIST MISSING;
	FORMAT admit disch YEAR4.;
RUN;

TITLE "VALIDATE COMPLETENESS VARIABLES";
PROC PRINT DATA = index03 (OBS=100);
  VAR hicno admit disch postmo postmod postmo_a postmod_a
      begin post begind postd para para_b premo premo_a death edbvdeth
    /*Y&YP5.BUY1-Y&YP5.BUY12 Y&YP5.HMO1-Y&YP5.HMO12*/
     Y&YP4.BUY1-Y&YP4.BUY12 Y&YP4.HMO1-Y&YP4.HMO12
     Y&YP3.BUY1-Y&YP3.BUY12 Y&YP3.HMO1-Y&YP3.HMO12
     Y&YP2.BUY1-Y&YP2.BUY12 Y&YP2.HMO1-Y&YP2.HMO12
     Y&YP1.BUY1-Y&YP1.BUY12 Y&YP1.HMO1-Y&YP1.HMO12
     Y&YYE.BUY1-Y&YYE.BUY&MM Y&YYE.HMO1-Y&YYE.HMO&MM;
  FORMAT admit disch MMDDYY10.;
RUN;

PROC SQL NOPRINT;
SELECT "'"||hicno||"'" INTO: coveragesample
	SEPARATED BY ","
FROM index03(OBS=100);
QUIT;
%PUT coverage sample hicnos:;
%PUT &coveragesample.;

TITLE "ETL COVERAGE DATA FOR COVERAGE SAMPLE RECORDS";
PROC PRINT DATA=data_ben.&coverage_dataset.;
	WHERE hicno IN (&coveragesample.);
RUN;

/*VALIDATE COVERAGE FOR PART A=0 AND PREMO_A>0************************/
DATA parta0;
	SET index03(OBS=50);
	WHERE para=0 and premo_a>0;
RUN;

TITLE "VALIDATE COMPLETENESS VARIABLES WHERE PARA=0 AND PREMO_A>0";
PROC PRINT DATA = parta0;
  VAR hicno admit disch postmo postmod postmo_a postmod_a
      begin post begind postd para para_b premo premo_a death edbvdeth
     /*Y&YP5.BUY1-Y&YP5.BUY12 Y&YP5.HMO1-Y&YP5.HMO12*/
     Y&YP4.BUY1-Y&YP4.BUY12 Y&YP4.HMO1-Y&YP4.HMO12
     Y&YP3.BUY1-Y&YP3.BUY12 Y&YP3.HMO1-Y&YP3.HMO12
     Y&YP2.BUY1-Y&YP2.BUY12 Y&YP2.HMO1-Y&YP2.HMO12
     Y&YP1.BUY1-Y&YP1.BUY12 Y&YP1.HMO1-Y&YP1.HMO12
     Y&YYE.BUY1-Y&YYE.BUY&MM Y&YYE.HMO1-Y&YYE.HMO&MM;
  FORMAT admit disch MMDDYY10.;
RUN;
PROC SQL NOPRINT;
SELECT "'"||hicno||"'" INTO: parta0
	SEPARATED BY ","
FROM parta0;
QUIT;
%PUT parta0 coverage sample hicnos:;
%PUT &parta0.;
TITLE "ETL COVERAGE DATA FOR PARTA0 COVERAGE SAMPLE RECORDS";
PROC PRINT DATA=data_ben.&coverage_dataset.;
	WHERE hicno IN (&parta0.);
RUN;



TITLE "IF ADMIT/DISCH ARE IN THE SAME MONTH, POSTMO SHOULD EQUAL POSTMOD";
PROC FREQ DATA = index03;
 TABLES flag_unequal flag_unequal_a /LIST MISSING;
RUN;

/* NO HOSPICE DATA AVAILABLE. BR.*/
/*TITLE "CHECK OF HOSPICE INDICATORS";                                           
PROC PRINT DATA=index03 (OBS=20);                                               
 VAR hicno case admit disch hsp_enr_prior_admit hsp_enr_on_admit                
        hsp_enr_prior_disch hsp_enr_on_disch hspbeg1-hspbeg30                   
        hspend1-hspend30;                                                       
 WHERE hspbeg1 NE .;                                                            
RUN;                                                                            
*/

/*****DROP COVERAGE AND HOSPICE CALCULATING VARIABLES******/
DATA dx_data.index03(SORTEDBY=hicno case)
  ;
 SET index03;
 DROP I admmo begin post begind postd dismo frstday 
     /*y&yp5.buy1-y&yp5.buy12 y&yp5.hmo1-y&yp5.hmo12*/
     y&yp4.buy1-y&yp4.buy12 y&yp4.hmo1-y&yp4.hmo12 
     y&yp3.buy1-y&yp3.buy12 y&yp3.hmo1-y&yp3.hmo12 
     y&yp2.buy1-y&yp2.buy12 y&yp2.hmo1-y&yp2.hmo12
     y&yp1.buy1-y&yp1.buy12 y&yp1.hmo1-y&yp1.hmo12
     y&YYE.buy1-y&YYE.buy&mm y&YYE.hmo1-y&YYE.hmo&mm
	 /*hspbeg1-hspbeg30 hspend1-hspend30*/
     flag_unequal flag_unequal_a
     ;
RUN;
**********************************************************************;

%macro nobs(data,name); /*Testing the observation counting macro. BR 5-10-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=dx_data.index02,name=obs); 
%nobs(data=data_an.coverage&YYE._&MM., name=obs);
%nobs(data=dx_data.index03 ,name=obs); 


/*DELETE WORK AND INITIAL DATASET TO CLEAN UP THE EG PROCESS FLOW*/
PROC DELETE DATA=index03 parta0; 
RUN;
TITLE;

DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 3_add_coverage_and_hospice" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
