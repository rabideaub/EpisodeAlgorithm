/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 00_build_stay_dataset

FOR USE WITH: ALL MEASURES

PURPOSE:      Build Stay level dataset 

OVERVIEW:     

INPUT DATA: 
	data_pta.&pta_in_base_dataset.
	data_pta.&pta_in_line_dataset.

OUTPUT FILES: 
	data_sty.&stay_dataset.

************************************************************************************************************/

OPTIONS COMPRESS=YES REUSE=YES MLOGIC NOMPRINT NOMACROGEN NOSYMBOLGEN STIMER FULLSTIMER OBS=MAX ;

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
put "  Job Name: 00_build_stay_dataset" /;
PUT "  Start Date: " StartDate ;
PUT "  Start Time: " StartTime ;
PUT "===================================================================================";
RUN;

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
%remove(ds=data_sty.&stay_dataset.);
/**********************************************************************************************
**********************************************************************************************/


**********************************************************************
* CHECK MACRO VARIABLES
**********************************************************************;
%PUT DXN (MAX NUMBER OF DIAGNOSIS)= &DXN.;
%PUT PRN (MAX NUMBER OF PROCEDURE)= &PRN.;
**********************************************************************;



/****************************************************************/
/*   CONVERT STATE CODE TO NUMERIC                              */
/*   RENAME LOS TO VA_LOS                                       */
/*   COUNT DIAG CODES                                           */
/****************************************************************/
DATA dx_data.temp0_inpat_base;
	SET data_pta.&pta_in_base_dataset.;

	pstate = substr(provid, 1,2);
	RENAME los = va_los;
	ARRAY a_clm_dgns_cd {&dxn} $ 7 diag1-diag&dxn;
	diag_count = 0;
	DO i = 1 TO &dxn BY 1;
		IF a_clm_dgns_cd[i] NE '' THEN diag_count = diag_count + 1;
	END;
	yr_disch=year(disch);
Run;

proc freq data=dx_data.temp0_inpat_base;
	tables yr_disch;
run;

 *********************************************************************;         
 * FROM COMBINED SAF INPATIENT FILE INCLUDING ONLY SHORT-STAY           
 * AND CRITICAL ACCESS HOSPITALS, TEMPORARILY SEPARATE OUT SINGLE-              
 * CLAIM AND MULTI-CLAIM HOSPITAL STAYS.                                        
 *********************************************************************;         
                                                                                
PROC SORT DATA=dx_data.temp0_inpat_base                          
          OUT=dx_data.TEMP1_INPAT; by hicno provid ;                                   
RUN;
 
DATA dx_data.temp2_prv1stay dx_data.temp2_delete dx_data.temp3_prvmstay;  
   SET dx_data.temp1_inpat;                                    
   BY hicno provid ;                                                            
    * Output single inpatient claim with given PROVID(=hospital) and 
      patient status is not 'still patient' to PRV1STAY;                        
   IF first.provid AND LAST.provid THEN DO;                                     
      REC1STAY=1;  
      IF DISST ne 'C' 
           THEN OUTPUT dx_data.temp2_prv1stay;                                          
      ELSE OUTPUT dx_data.temp2_delete;                                                              
   END;                                                                         
   * If more than one claim with same PROVID, then multi-claim stay;            
   ELSE OUTPUT dx_data.temp3_prvmstay; 
RUN;


 *********************************************************************;         
 * FOR BENEFICIARIES WITH THE SAME PROVIDER, IDENTIFY INTERIM           
 * CLAIMS: IF THEY HAVE THE SAME ADMISSION DATES OR IF THE                      
 * ADMISSION DATE OF THE CLAIM IS EQUAL TO, OR ONE DAY AFTER, THE               
 * DISCHARGE DATE OF THE PREVIOUS CLAIM, AND THE STATUS CODE OF THE             
 * PREVIOUS CLAIM IS "STILL PATIENT".                                           
 *********************************************************************;         
                                                                                
PROC SORT DATA=dx_data.TEMP3_PRVMSTAY OUT=dx_data.TEMP4_PRVMSTAY;                                
   BY HICNO PROVID ADMIT DISCH diag_count;  
RUN;                                  
     
 
DATA dx_data.OUT1_MRECSTAY (DROP=lastfrom lastthru last_ddest) ;                          
	SET dx_data.TEMP4_PRVMSTAY;                                                          
	BY hicno provid ADMIT DISCH;                                        
	                                                                            
	ATTRIB staynum  LENGTH=5  LABEL='Inpatient stay number (sequential)';        
	ATTRIB lastfrom LENGTH=5  LABEL='CLAIM FROM DATE OF LAST CLAIM';             
	ATTRIB lastthru LENGTH=5  LABEL='CLAIM THROUGH DATE OF LAST CLAIM';          
	ATTRIB last_ddest LENGTH=$2 LABEL='Claim status code of last claim';           
	                                                                            
	RETAIN staynum 0 lastfrom lastthru last_ddest;                                 
	                                                                            
	IF FIRST.provid THEN DO;                                                     
		staynum = staynum + 1;                                                 
		lastfrom = admit;                                                      
		lastthru = disch;                                                      
		last_ddest = ddest;                                                    
	END;                                                                         
	ELSE DO;                                                                     
		/* IF ADM DATES ARE THE SAME, THEN TAKE THE LAST RECORD */                 
		IF admit = lastfrom AND disch >= lastthru THEN DO;                         
		    lastthru = disch;                                                   
		    last_ddest = ddest;                                                 
		END;                                                                       
		                                                                        
		 /* Abutting claims, with status code of earlier claim */               
		 /* equal to "still a patient"                         */               
		 /* will have same staynum */                                           
		ELSE IF (admit = lastthru + 1 OR admit=lastthru) AND                       
		    (30 <= INPUT(last_ddest, 2.) <= 39) THEN DO;                          
			lastthru = disch;                                                   
			last_ddest = ddest;                                                 
		END;                                                                       
		                                                                
		 /* Not part of the same inpatient stay */                              
		 /* will have different staynum */                                      
		ELSE DO;                                                                   
		    staynum = staynum + 1;                                              
		    lastfrom = admit;                                                   
		    lastthru = disch;                                                   
		    last_ddest = ddest;                                                 
		END;                                                                       
	END;                                                                          
RUN;                                                                            
                                                                                
PROC SORT DATA=dx_data.out1_mrecstay OUT=dx_data.temp5_mrecstay;                               
   BY hicno staynum admit disch diag_count;  
RUN;                                 


 *********************************************************************;         
 * Collapse interim claims belonging to same stay into a single         
 * claim. The admission date comes from the first claim of a stay               
 * and discharge date comes from the last claim of a stay. ICU and CCU          
 * indicators were taken from a claim which has the value of 1.                 
 * All other variable values come from the last discharge claim                 
 * in a series of interim claims.                                               
 *********************************************************************;         

DATA dx_data.temp6_psmedq (DROP=lastfrom lastthru last_ddest lasticu lastccu) 
     dx_data.temp6_delete1 
	 dx_data.temp6_delete2 
	 dx_data.temp6_delete3 
	 dx_data.temp6_delete4
	 ;
	SET dx_data.temp5_mrecstay;                                                        
	BY hicno staynum admit disch;                                       
	                                                                        
	LABEL                                                                        
		admit    = 'from date'                                                    
		disch    = 'through date'                                                 
		ddest    = 'status code' 
		clm_admsn_dt = 'actual admission date' 
		;                                                                         
	LENGTH                                                                       
		lastfrom 5                                                                
		lastthru 5                                                                
		last_ddest $ 2                                                              
		;                                                                         
	RETAIN                                                                       
		lastfrom lastthru last_ddest                                                
		lasticu lastccu                                                           
		;                                                                         
	/* if only one claim for this inpatient stay */                               
	IF FIRST.staynum AND LAST.staynum THEN DO;                                      
		rec1stay=1;                                                                
		IF disst NE 'C' THEN OUTPUT dx_data.temp6_psmedq;                                            
		ELSE OUTPUT dx_data.temp6_delete1;                                                               
	END;                                                                            
                                                                            
	ELSE DO;  
		IF FIRST.staynum THEN DO;                                                    
			lastfrom = admit;                                                            
			lastthru = disch;                                                            
			last_ddest = ddest; 
			lasticu = icuind; 
			lastccu = ccuind; 
			OUTPUT dx_data.temp6_delete2;                                                               
		END;                                                                         
		ELSE DO;
			IF disch >= lastthru THEN DO; 
				lastthru = disch; 
				last_ddest = ddest; 
				icuind = MAX(icuind,lasticu); 
				ccuind = MAX(ccuind,lastccu); 
				lasticu=icuind; 
				lastccu=ccuind; 
			END;                                                                   
			IF last.staynum THEN DO;                           
				IF disst NE 'C' THEN DO;
					admit = lastfrom;                                                   
					disch = lastthru;                                                   
					ddest = last_ddest;                                                 
					icuind=lasticu;                                                        
					ccuind=lastccu; 
					rec1stay=0;                                                         
					OUTPUT dx_data.temp6_psmedq;
				END; 
				ELSE DO;
					OUTPUT dx_data.temp6_delete3;                                                               
				END; 
			END;                                                                
			ELSE DO;
				OUTPUT dx_data.temp6_delete4;                                                               
			END;                                                                   
		END;                                                                         
	END;   
RUN;

 *********************************************************************;         
 * Combine single provider hospital stays with multi-claim              
 * hospital stays to create a stay file.                               
 *********************************************************************;         
                                                                                
DATA dx_data.temp7_psmed;
	SET dx_data.temp2_prv1stay  dx_data.temp6_psmedq;
	los = (disch - admit);
	IF los = 0 THEN los = 1;
RUN; 


/****************************************************************/
/*   Drop Stays over 365 days                                   */
/****************************************************************/
DATA dx_data.temp8_psmed dx_data.temp9_los_365;
	SET	dx_data.temp7_psmed;
    IF los > 365 THEN OUTPUT dx_data.temp9_los_365;
	ELSE OUTPUT dx_data.temp8_psmed;
RUN;


/****************************************************************/
/*   Drop Extra Columns add txflag                */
/****************************************************************/
DATA dx_data.part_a_pre_final_base;
   SET dx_data.TEMP8_PSMED;
     /*Create TXFLAG*/
     IF ddest IN (2,66) THEN txflag=-1;  /*Transfer to another hospital or CAH*/
     ELSE IF ddest=20 THEN txflag=1;     /*Died*/
     ELSE txflag=0;                             /*Otherwise*/

     LABEL
           txflag = "-1=TRANSFER, 1=DIED, 0=OTHERWISE"
           hicno  = "CLEAN XREFD ID"
     ;
RUN; 


/****************************************************************/
/*   Pre-Final Sort                                             */
/****************************************************************/
PROC SORT DATA=dx_data.part_a_pre_final_base OUT=dx_data.part_a_final_base;
     BY hicno admit disch txflag provid;
RUN;


***-----------------------------------***;
***  Add Number Unique Sequential ID  ***;
***-----------------------------------***;
DATA dx_data.part_a_final_base; 
	SET dx_data.part_a_final_base; 
	nobs = _N_;
	LABEL
		admit       = 'From date'                                                    
		disch       = 'Through date'                                                 
		ddest       = 'Status code' 
		clm_admsn_dt = 'Actual Admission Date' 
		;
RUN;

                                                                               
 *********************************************************************;         
 * Sort BASE file to remove duplicate records.                              
 *********************************************************************;         
 
PROC SORT DATA=dx_data.part_a_final_base OUT=dx_data.part_a_final_base_dedup NODUPKEY;                       
	 BY hicno admit disch provid 
	    diag1-diag&dxn 
	    proc1-proc6                 
	    procdt1-procdt6                 
	    ddest disst ccuind icuind admsour /*mscd*/         
	    typeadm drgcd rec1stay
	    npi_at npi_op;            
RUN;                                                                            
                                                                                
 *********************************************************************;         
 * Sort BASE file by HICNO, admission date, discharge date, and             
 * PROVID to facilitate identification of overlapping claims.                   
 *********************************************************************;         
                                                                                
DATA dx_data.find_overlapping_claims;  
	SET dx_data.part_a_final_base_dedup;                                         
	KEEP hicno admit disch nobs provid case_type;       
RUN;
 
PROC SORT;  BY hicno admit disch provid; 
RUN; 
                                                                                
*********************************************************************;          
* Create a permanent data file (MARKER) that contains                 
* two variables - YXXNOBS (claims observation number) and CASENO                
* (overlapping claims case number).                                             
*********************************************************************;          
                                                                                
DATA dx_data.marker (KEEP=newobs                                                                    
					      caseno     /**UNIQUE # FOR A GROUP OF OVERLAPPING CLAIMS**/               
					      case_type
					      RENAME=(NEWOBS=NOBS));                                               
                                                                                
	LENGTH HICNO2 $ 15 MARK $ 1 PROVID2 $ 6;                                         
	RETAIN MARK ' ';                                                                
                                                                                
	/**DEFINE LAGGED VARIABLES**/                                           
	PROVID2=PROVID;                                                              
	HICNO2=HICNO;                                                              
	ADMDATE2=ADMIT;                                                            
	DISDATE2=DISCH;                                                            
	NOBS2=NOBS;                                                                
                                                                                
	*********************************************************************;          
	* Set up algorithm to identify overlapping claims.                      
	* Overlapping claims are those claims where: 1) HICNOs are the same;            
	* and 2) Admission date is less than or equal to the discharge date.            
	*********************************************************************;          
	                                                                            
	SET dx_data.find_overlapping_claims;                                                               
	/**DEFINE CRITERIA FOR OVERLAPPING CLAIMS**/                                   
	IF (                                                                            
		 (HICNO2=HICNO)                                                            
		 AND                                                                            
			((PROVID2 = PROVID AND ADMIT = ADMDATE2 AND DISCH = DISDATE2)     
			OR                                                                             
			(ADMDATE2 LE ADMIT LT DISDATE2))                                          
	    )                                                                           
	THEN DO;                                                                        
	    IF MARK=' ' THEN DO;                                                        
	        CASENO+1;                                                               
	        NEWOBS=NOBS2;                                                           
	        OUTPUT;                                                                 
	        NEWOBS=NOBS;                                                       
	        OUTPUT;                                                                 
	        MARK='1';                                                               
	    END;                                                                        
	    ELSE IF MARK='1' THEN DO;                                                   
	        NEWOBS=NOBS;                                                       
	        OUTPUT;                                                                 
	    END;                                                                        
	END;                                                                            
	ELSE MARK=' ';                                                                  
RUN;                                                                            
                                                                                
TITLE "OVERLAPPING CLAIMS ID FILE";                                        
PROC CONTENTS DATA=dx_data.marker;                                              
RUN;       
 
                                                                                
                                                                               
*********************************************************************;          
* Frequency on CASENO variable and output to a temp file                
* (dx_data.tempcnt).  Run means on the counts in the temp file.                         
*********************************************************************;          
                                                                                
PROC FREQ DATA=dx_data.marker NOPRINT;                                          
	TABLES CASENO / OUT=dx_data.tempcnt(DROP=PERCENT);                                      
RUN; 
 
TITLE "OVERLAPPING CLAIMS DELETIONS";                                      
TITLE2 "STATS ARE FOR VARIABLE COUNT:";                                         
TITLE3 "THE # OF CLAIMS IN EACH OVERLAPPING EPISODE";                           
PROC MEANS DATA=dx_data.tempcnt N MIN MAX MEAN STD;                                     
VAR COUNT;                                                                      
RUN;                                                                            
                                                                                
TITLE2;
TITLE3; 
                                                                               
*********************************************************************;          
* Sort the MARKER and BASE files by observation number            
* (QXXNOBS) to be able to merge this information to identify                    
* overlapping claims and remove them.  
*********************************************************************;          
                                                                                
PROC SORT DATA=dx_data.marker;                                                  
	BY nobs;                                                                   
RUN;                                                                            
                                                                                
PROC SORT DATA=dx_data.part_a_final_base_dedup OUT=dx_data.part_a_final_base_temp;                              
	BY nobs;                                                                   
RUN;                                                                            
                                                                                
                                                                                
*********************************************************************;          
* Merge MARKER and BASE file by QXXNOBS. Non-matches              
* go into the new MedPAR file (PSMPEXX) - non-overlapping claims.               
* Delete records with admission date longer than 1 year prior to                
* discharge date, i.e., LOS > 365.    
*********************************************************************;          
                                                                                
DATA data_sty.&stay_dataset.                                                             
   (SORTEDBY=nobs LABEL="part_a_final_base_no_overlaps: NO OVERLAPS") 
     dx_data.delete_overlaps;
	MERGE dx_data.part_a_final_base_temp (IN=A)                                                  
	      dx_data.marker (IN=B KEEP=nobs);                                   
	BY nobs;

	IF A AND NOT(B) THEN OUTPUT data_sty.&stay_dataset.;
	ELSE OUTPUT dx_data.delete_overlaps;
RUN; 
                                                                                
PROC CONTENTS DATA=data_sty.&stay_dataset.;                                              
RUN;    
 
                                                                               
TITLE "PSMEDE";
PROC FREQ DATA=data_sty.&stay_dataset.;                                                  
TABLES case_type * (rec1stay disst typeadm admsour los ddest icuind ccuind)
       /LIST MISSING;
RUN; 

  TITLE "CASE_TYPE ON MARKER";
PROC FREQ DATA=dx_data.marker;
  TABLES CASE_TYPE/MISSING;
RUN;   


/****************************************************************/
/*   Final Sort                                                 */
/****************************************************************/
PROC SORT DATA=data_sty.&stay_dataset.;
     BY hicno admit disch txflag provid;
RUN;

***-----------------------------------***;
***  Add nobs                         ***;
***-----------------------------------***;
DATA data_sty.&stay_dataset.; 
	SET data_sty.&stay_dataset. (DROP=nobs);
	nobs = _N_;
RUN;

data test;
	set data_sty.&stay_dataset.; 
	year_disch = year(disch);
run;

proc freq data= test;
	tables year_disch;
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
%nobs(data=data_pta.&pta_in_base_dataset., name=obs); 
%nobs(data=data_sty.&stay_dataset., name=obs); 



DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 00_build_stay_dataset"/;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
