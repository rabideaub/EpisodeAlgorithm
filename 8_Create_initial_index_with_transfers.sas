/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 1_Create_initial_index_with_transfers

FOR USE WITH: AMI, HF, PNEUMPONIA, STROKE AND COPD

PURPOSE:      Identify cases for the readmission index and define the transfer chains 

OVERVIEW: 	  Select cases that have a qualifying diagnosis in reporting period and select those
			  cases that are related to the selected cases by transfering in or out.


INPUT DATA: 
	data_sty.&stay_dataset.        *PRE-SORTED BY HICNO ADMIT DISCH TXFLAG PROVID  -WITH NOBS CREATED AFTER SORT
	data_an.flag_transfers         *PRE-SORTED BY HICNO TRANS_CASENO

OUTPUT FILES: 
	dx_data.index01

************************************************************************************************************/

OPTIONS MPRINT COMPRESS=YES;

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
PUT "  Job Name: 1_Create_initial_index_with_transfers" /;
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
%remove(ds=dx_data.index01);
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
RUN;
**********************************************************************;



**********************************************************************
*GET LIST OF DIAG CODES FOR THE MEASURE
**********************************************************************;
**********************************************************************;
**********************************************************************
*GET STARTING DATASET OF CLAIMS IN REPORTING PERIOD AND SIX MONTHS 
*PRIOR FOR BENES WITH QUALIFYING DIAGNOSIS IN REPORTING PERIOD
**********************************************************************;
/*GET LIST OF HICNOS FOR BENES WITH QUALIFYING DIAGNOSIS DISCHARGES IN REPORTING PERIOD*/
DATA dx_data.&DX._benes(KEEP=hicno);
   SET data_sty.&stay_dataset.(KEEP=hicno disch diag1 diag2 proc1 drgcd);

	WHERE disch >= &BEGINDT AND disch <= &CLOSEDT
	AND &&DIAGSELECT_&DX;
RUN;

PROC SORT DATA=dx_data.&DX._benes NODUPKEY; BY hicno;
RUN;

/*SELECT ALL STAYS FOR IDENTIFIED BENES WITHIN REPORTING PERIOD AND SIX MONTHS PRIOR*/
DATA dx_data.startingclaims;
	MERGE dx_data.&DX._benes(IN=A)
	data_sty.&stay_dataset.(WHERE=(disch >= INTNX('MONTH',&BEGINDT,-6) AND disch <= &CLOSEDT)
					   KEEP= admit admsour ccuind ddest diag1-diag&dxn disch disst 
						drgcd dvrsnd01-dvrsnd25 edgscd01-edgscd12 evrscd01-evrscd12 hicno icuind 
						los mdcl_rec mdcl_rec /*mscd*/ npi_at npi_op poaend01-poaend12 poancd1-poancd25 
						proc1-proc&prn procdt1-procdt&prn provid pstate pvsncd01-pvsncd25 txflag
						typeadm upin_at upin_op nobs sslssnf pmt_amt tot_chg factype);
	BY hicno;
	IF A;
RUN;
**********************************************************************;


**********************************************************************
* FLAG TRANSFER CHAINS WHERE LAST STAY IN TRANSFER CHAIN IS NOT A QUALIFYING DIAGNOSIS 
* OR IS NOT IN REPORTING PERIOD
**********************************************************************;
/*CREATE TABLE WITH ONE RECORD PER TRANSFER CHAIN*/
DATA flag_chains_mort;
	SET data_an.flag_transfers;
	BY hicno trans_caseno;
	RETAIN mort_first_disch_nonQ mort_one_diag_nonQ index_stay_ct;

	/*FIRST DISCHARGE MUST BE WITHIN REPORTING PERIOD OR PRIOR SIX MONTHS FOR MORTALITY*/
	IF FIRST.trans_caseno THEN DO;
		index_stay_ct = 0;
		mort_one_diag_nonQ = 0;
		IF disch < INTNX('MONTH',&BEGINDT,-6) THEN mort_first_disch_nonQ = 1; 
		  ELSE mort_first_disch_nonQ = 0;
	END;

	/*COUNT OF STAYS IN INDEX PERIOD*/
	IF disch >= INTNX('MONTH',&BEGINDT,-6) AND disch <= &CLOSEDT THEN index_stay_ct +1;
		
	/*ALL STAYS IN TRANSFER CHAIN MUST HAVE QUALIFYING DIAGNOSIS FOR MORTALITY*/
	IF ~(&&DIAGSELECT_&DX)
	THEN mort_one_diag_nonQ = 1;

	IF LAST.trans_caseno THEN OUTPUT;
RUN;

/*CREATE FLAG FOR INELIGIBLE READMISSION TRANSFER CHAINS*/
DATA flag_chains_read;
	SET data_an.flag_transfers;
	BY hicno trans_caseno;
	WHERE disch <= &CLOSEDT;
	/*LAST STAY IN TRANSFER CHAIN MUST BE A QUALIFYING DIAGNOSIS IN REPORTING PERIOD*/ 
	IF LAST.trans_caseno THEN DO;
		IF ~(&&DIAGSELECT_&DX)
		  OR disch < &BEGINDT
			THEN last_diag_nonQ = 1; 
		ELSE last_diag_nonQ = 0; 
		OUTPUT;
	END;
RUN;

/*COMBINE TRANSFER FLAG DATASETS*/
DATA flag_chains;
	MERGE flag_chains_mort 
		  flag_chains_read(KEEP = hicno trans_caseno last_diag_nonQ);
	BY hicno trans_caseno;
RUN;
**********************************************************************;



**********************************************************************
* ADD SEQUENTIAL GROUP NUMBERS
**********************************************************************;
DATA count_trans;
	SET flag_chains;
	BY hicno;

	LENGTH group 3;
	IF FIRST.hicno THEN group=0;
	/*ITERATE GROUP NUMBERS FOR TRANSFER CHAINS THAT */ 
	/*HAVE MORE THAN ONE QUALIFYING STAY IN THE INDEX PERIOD*/	
	IF last_diag_nonQ = 0 AND index_stay_ct > 1 THEN group+1;  
RUN;

DATA tx_with_group (KEEP=nobs group trans_caseno last_diag_nonQ mort_first_disch_nonQ 
						 mort_one_diag_nonQ index_stay_ct);
	MERGE data_an.flag_transfers 
		  count_trans(KEEP=hicno trans_caseno group last_diag_nonQ mort_first_disch_nonQ 
						   mort_one_diag_nonQ index_stay_ct);
	BY hicno trans_caseno;
RUN;

PROC SORT;
	BY nobs;
RUN;
**********************************************************************;



**********************************************************************
* MERGE THESE TRANSFER CLAIMS ONTO UNBUNDLED CLAIMS AND FLAG ALL
* CLAIMS WITHIN A TRANSFER CHAIN.
**********************************************************************;
DATA transfers_flagged 
	 dx_data.dropped_cases;
	MERGE dx_data.startingclaims(IN=SC) tx_with_group(IN=T);
	BY nobs;
	IF SC;

	IF index_stay_ct>1 THEN trans=1;
	ELSE DO;
		group=0;
		trans=0;
	END;

	FORMAT dropreason dropreasonMORT $60.;
	dropreason='';
	dropreasonMORT = '';

	IF (&&DIAGSELECT_&DX)
	THEN dxdiag=1;
	  ELSE dxdiag=0;

	/*DROP NON TRANSFER NON DX CASES*/
	IF trans=0 AND dxdiag=0
		THEN dropreason = '1. non transfer non DX case';

	/*DROP ALL STAYS IN TRANSFER CHAINS THAT DO NOT END IN A QUALIFYING STAY*/
	ELSE IF last_diag_nonQ = 1
		THEN dropreason = '2. trans chain did not end in dx stay in rpt period';

	/*DROP NON TRANSFER STAYS FROM BEFORE REPORTING PERIOD*/
	ELSE IF trans=0 AND DISCH < &BEGINDT.
		THEN dropreason = '3. non trans stay from before reporting period';


	/*MORTALITY SPECIFIC DROPREASONS  - STAYS FLAGGED FOR REMOVAL FROM MORTALITY INDEX*/
	/**********************************************************************************/
	/*DROP ALL STAYS IN TRANSFER CHAINS WITH FIRST STAY HAVING DISCH MORE THAN 6 MONTHS PRIOR TO REPORTING PERIOD*/
	IF mort_first_disch_nonQ = 1 THEN dropreasonMORT = '1. FIRST DISCH IN TRANS CHAIN PRIOR TO PERIOD';

	/*DROP ALL STAYS IN TRANSFER CHAINS HAVING AT LEAST ONE STAY NOT MEET MEDICAL CRITERIA BEFORE, DURING OR AFTER INDEX PERIOD*/
	ELSE IF mort_one_diag_nonQ = 1 THEN dropreasonMORT = '2. ONE OR MORE NON DX STAY IN TRANS CHAIN';

	IF dropreasonMORT = '' THEN mort_flag = 1; 
	  ELSE mort_flag = 0;

	/*FORMAT DROPREASONMORT FOR STAYS KEPT IN MORTALITY INDEX*/
	IF dropreasonMORT = '' THEN dropreasonMORT = 'Included in Mortality Index';

	IF dropreason='' THEN OUTPUT transfers_flagged;
	  ELSE OUTPUT dx_data.dropped_cases;
RUN;

TITLE 'CASES BEFORE DROPS';
PROC SQL;
SELECT COUNT(1)
FROM dx_data.startingclaims;
QUIT;

TITLE 'CASE DROP REASONS';
PROC FREQ DATA=dx_data.dropped_cases;
	TABLE dropreason /list missing;
RUN;

TITLE 'CASES INCLUDED IN READMISSION INDEX AFTER DROPS';
PROC SQL;
SELECT COUNT(1)
FROM transfers_flagged;
QUIT;

TITLE 'NON TRANSFER NON DX CASE';
PROC FREQ DATA=dx_data.dropped_cases;
	TABLE trans*dxdiag /list missing;
	WHERE dropreason = '1. non transfer non DX case';
RUN;


DATA dropped_cases2;
	SET dx_data.dropped_cases;
	BY trans_caseno;
	WHERE dropreason = '2. trans chain did not end in dx stay in rpt period';

	if last.trans_caseno;
RUN;


TITLE 'TRANS CHAIN DID NOT END IN DX STAY IN RPT PERIOD';
TITLE2 'NOTE: MISSING DISCH ARE TRANSFER CHAIN CASES THAT WERE NOT PART OF STARTINGCLAIMS';
PROC FREQ DATA=dropped_cases2;
	TABLE dxdiag*DISCH /list missing;
	FORMAT disch MONYY.;
RUN;

TITLE 'NON TRANS STAY FROM BEFORE REPORTING PERIOD';
PROC FREQ DATA=dx_data.dropped_cases;
	TABLE trans*DISCH /list missing;
	WHERE dropreason = '3. non trans stay from before reporting period';
	FORMAT disch MONYY.;
RUN;

TITLE 'MORTALITY INDEX DROP REASONS (FROM READMISSION INDEX)';
PROC FREQ DATA=transfers_flagged;
	TABLE dropreasonMORT /list missing;
RUN;
**********************************************************************;



**********************************************************************
* ADD CASE NUMBERS 
**********************************************************************;
PROC SORT DATA=transfers_flagged;
	BY hicno admit disch;
RUN;

DATA transfers_flagged_cases;
	SET transfers_flagged;
	BY HICNO;

	LENGTH case 3;

	IF FIRST.hicno THEN case=0;

	case+1;
RUN;
**********************************************************************;



**********************************************************************
* FLAG FIRST STAY IN A TRANSFER, LAST STAY IN A TRANFSER, AND TRANSFER
* CLAIMS THAT ARE NEITHER FIRST NOR LAST 
**********************************************************************;
PROC SORT DATA=transfers_flagged_cases;
	BY hicno group case;
RUN;

DATA dx_data.index01 (KEEP= admit admsour case history_case ccuind ddest diag1-diag&dxn   
						disch disst drgcd dvrsnd01-dvrsnd25 edgscd01-edgscd12 evrscd01-evrscd12
						group hicno icuind los mdcl_rec mdcl_rec /*mscd*/ npi_at npi_op 
						poaend01-poaend12 poancd1-poancd25 proc1-proc&prn procdt1-procdt&prn provid 
						pstate pvsncd01-pvsncd25 trans trans_first trans_last trans_mid txflag
						typeadm upin_at upin_op year mort_flag sslssnf pmt_amt tot_chg factype
					  SORTEDBY=hicno group case);
	SET transfers_flagged_cases;
	BY hicno group case;

	/*CREATE YEAR VARIABLE - missing for prior to period*/ 

	LENGTH year $4;
	year=year(disch);/*Changed year to match calendar year instead of medicare
					   enrollment year. BR 7/23/15*/
	/*
	IF "1jul20&yy"d <= disch <= "30jun%eval(20&yy+1)"d               
	  THEN year = "&yy"||SUBSTR("%EVAL(20&yy+1)",3,2);             
	ELSE IF "1jul%eval(20&yy+1)"d <= disch <= "30jun%eval(20&yy+2)"d 
	  THEN year = SUBSTR("%EVAL(20&yy+1)",3,2)||SUBSTR("%EVAL(20&yy+2)",3,2);                     
	ELSE IF "1jul%eval(20&yy+2)"d <= disch <= "30jun%eval(20&yy+3)"d 
	  THEN year = SUBSTR("%EVAL(20&yy+2)",3,2)||SUBSTR("%EVAL(20&yy+3)",3,2);  
	*/ 

	/*DEFINE TRANSFER FLAGS*/
	trans_first=0;
	trans_last=0;
	trans_mid=0;
	IF trans=1 THEN DO;
		IF FIRST.group THEN trans_first=1;
		IF LAST.group THEN trans_last=1;
		IF trans_first=0 AND trans_last=0 THEN trans_mid=1;
	END;

	LENGTH history_case 3;
	RETAIN history_case;

	/*DEFINE HISTORY_CASE*/
	IF group=0 THEN history_case=case;
	ELSE IF FIRST.group THEN history_case=case;

RUN;
**********************************************************************;


********************************************************************; 
*CONFIRM THAT TRANSFER CHAINS HAVE A FIRST AND LAST FLAG
********************************************************************; 
DATA first_last(KEEP=hicno group first last both);                            
	SET dx_data.index01(KEEP=hicno group trans_first trans_last);                                                                  
	BY hicno group;   
	WHERE group NE 0;                                                            

	IF FIRST.group THEN DO;                                                      
		first=0;                                                                   
		last=0;                                                                    
	END;     

	IF trans_first=1 THEN first+1;                                               
	IF trans_last=1 THEN last+1;    

	IF LAST.GROUP THEN DO;                                                       
		IF first=last THEN both=1;                                                 
		OUTPUT;                                                                    
	END;                                                                         
RUN;                                                                           
                                                            
TITLE "CHECK OF TRANSFER FIRST AND LAST FLAGS";
PROC FREQ DATA=first_last;
	TABLES first*last*both/LIST MISSING;
RUN;

TITLE "PRINT RECORDS WHERE BOTH DOES NOT EQUAL 1";
PROC PRINT DATA=first_last (WHERE=(both NE 1));
RUN;
**********************************************************************;



**********************************************************************
* RUN ADDITIONAL QA FREQS AND PRINTS ON index01 FILES FOR CHECKING
* TRANSFER CODING FOR BEGINNING OF STUDY PERIOD.
**********************************************************************;
TITLE "index01: &dx. DISCH ALL CHECK";
PROC FREQ DATA=dx_data.index01;
	TABLES group*trans*txflag
	      year*disch case*history_case
	      group*(trans_first trans_mid trans_last case)
		  trans_first*trans_mid*trans_last
		  mort_flag
	/LIST MISSING;
	FORMAT disch MONYY.;
RUN;

TITLE "index01: &dx. DISCH PRIOR TO 07/01/20&YY";
PROC FREQ DATA=dx_data.index01;
	TABLES group*trans*txflag
	      disch
	      group*(trans_first trans_mid trans_last case)
	/LIST MISSING;
	WHERE disch LT MDY(07,01,20&YY);
	FORMAT disch MONYY.;
RUN;

TITLE "&dx.: DIAG1 among TRANS=0";
PROC FREQ DATA=dx_data.index01;
	TABLES diag1/LIST MISSING;
	WHERE trans=0;
RUN;

TITLE "&dx.: DIAG1 among TRANS_LAST=1";
PROC FREQ DATA=dx_data.index01;
	TABLES diag1/LIST MISSING;
	WHERE trans_last=1;
RUN;

PROC SORT
	DATA=dx_data.index01  (KEEP=hicno admit disch)
	OUT=PRE_DiSCH (KEEP=hicno)
	NODUPKEY;
	BY hicno;
	WHERE disch LT MDY(07,01,20&YY);
RUN;

DATA pre_disch_check;
	MERGE dx_data.index01 (IN=A) pre_disch (IN=B);
	BY hicno;
	IF A AND B;
RUN;

TITLE "&DX. QA PRINT OF DISCHARGES BEFORE BEGINNING OF STUDY PERIOD";
PROC PRINT DATA=pre_disch_check (OBS=50);
	BY hicno;
	VAR case group hicno admit disch provid
		trans_first trans_mid trans_last diag1 ddest;
	FORMAT admit disch MMDDYY10.;
RUN;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-10-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=data_sty.&stay_dataset.,name=obs); 
%nobs(data=dx_data.&DX._benes ,name=obs);
%nobs(data=dx_data.startingclaims ,name=obs); 
%nobs(data=data_an.flag_transfers ,name=obs); 
%nobs(data=dx_data.index01 ,name=obs); 


**********************************************************************;



/*DELETE WORK AND INITIAL DATASET TO CLEAN UP THE EG PROCESS FLOW*/
PROC DELETE DATA= flag_chains_mort flag_chains_read flag_chains count_trans 
				  transfers_flagged tx_with_group transfers_flagged_cases 
				  dropped_cases2 PRE_DiSCH pre_disch_check first_last;
RUN;
TITLE;
TITLE2;

DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 1_Create_initial_index_with_transfers" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
