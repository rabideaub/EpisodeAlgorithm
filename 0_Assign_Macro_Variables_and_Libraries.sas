**********************************************************************
* ASSIGN MACRO VARIABLES
**********************************************************************;
/*SET MEASURE VARIABLE  MI HF PN CD SK HW HKC HKR*/
%LET DX = SK_DRG;


%LET DIAGSELECT_MI = DIAG1 in('41000','41001','41010','41011','41020','41021','41030','41031','41040','41041','41050',
'41051','41060','41061','41070','41071','41080','41081','41090','41091','4111','41181','41189','4130','4131','4139');

%LET DIAGSELECT_HF = DIAG1 in('40201','40211','40291','40401','40403','40411','40413','40491','40493','4280','4281',
'42820','42821','42822','42823','42830','42831','42832','42833','42840','42841','42842','42843','4289','51881',
'51882','51884','7991');

%LET DIAGSELECT_PN = DIAG1 in('4800','4801','4802','4803','4808','4809','481','4820','4821','4822','48230','48231','48232',
'48239','48240','48241','48242','48249','48281','48282','48283','48284','48289','4829','4830','4831','4838','485',
'486','4870','48811','488','48801','48242','99731','99739','507','5070','5071','5078','99591','99592','038','0380','0381',
'03810','03811','03812','03819','0382','0383','0384','03840','03841','03842','03843','03844','03849','0388','0389','78552',
'51881','51882','51884','7991');

%LET DIAGSELECT_SK = DIAG1 in('43301','43311','43321','43331','43381','43391','43401','43411','43491','436','36234','430','431','4320',
'4321','4329','43300','43310','43320','43330','43340','43350','43360','43370','43380','43390','43400',
'4350','4351','4352','4353','4358','4359','4380','43810','43811','43812','43819','43820','43821','43822',
'43830','43831','43832','43840','43841','43842','43850','43851','43852','43853','4386','4387','43881',
'43882','43883','43884','43885','43889','4389');

%LET DIAGSELECT_SK_DRG = DRGCD in("061","062","063","064","065","066");

%LET DIAGSELECT_KH = PROC1 in('8151','8154','8152','8153','0070','0071','0072','0073','0085','0086','0087','8155',
				   '8159','0080','0081','0082','0083','0084');

%LET DIAGSELECT_CD = (DIAG1 in('49121','49122','4918','4919','4928','49320','49321','49322','496') |
(DIAG1 in('51881','51882','51884','7991') AND DIAG2 in('49121', '49122', '49321', '49322')));

%LET DIAGSELECT_FH = prxmatch("/^820.{1,}/",DIAG1)>0;


/*ASSIGN VALUES FOR MAX NUMBER OF DIAGNOSIS AND PROCEDURE CODES*/
%LET DXN = 25;
%LET PRN = 25;

/*ASSIGN FIRST YEAR OF REPORTING PERIOD 12 FOR HW*/
%LET YY = 03;

/*ASSIGN LAST YEAR OF REPORTING PERIOD*/
%LET YYE = 14;

/*END MONTH FOR COVERAGE DATA*/
%LET MM=12;

/*ESTABLISH THE TIMESET OF THE REPORTING WINDOW - CALENDAR YEAR, FISCAL YEAR, MEDICARE ENROLLMENT YEAR, ETC.
  CY GOES FROM 1/1/YY - 12/31/YYE, MEY GOES FROM 7/1/YY - 6/31/YYE, FY GOES FROM 10/1/YY-9/31/YYE.*/
%LET BEGINDT = MDY(01,01,20&YY.);
%LET CLOSEDT = MDY(12,31,20&YYE.);

/*REPORTING PERIOD END DATE - USED FOR HKC AND HKR*/
/*FOR readmission, ENDDATE= MDY(07,01,20&YYE)  FOR complication, ENDDATE= MDY(04,01,20&YYE) */
%LET ENDDATE = ;

/*REPORTING PERIOD START DATE - USED FOR HKC AND HKR*/
/*For readmission, STARTDT= MDY(07,01,20&YY)  For complication, STARTDT=MDY(04,01,20&YY)*/
%LET STARTDT= ;

/*CLAIM START DATE - SIX MONTHS PRIOR TO REPORTING PERIOD TO PICK UP PRIOR TRANSFERS - USED FOR HKC AND HKR*/
/*For readmission, PRIORDT= MDY(01,01,20&YY)  For complication, PRIORDT=MDY(10,01,(20&YY.-1))*/
%LET PRIORDT=;
**********************************************************************;



**********************************************************************
* LIBRARY LOCATIONS
**********************************************************************;
/*MEDPAR RAW DATA - ROOT DIRECTORY, NO YEAR COMPONENT*/
%LET med = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/PAC/Data/Raw/MedPAR;

/*DENOMINATOR RAW DATA - ROOT DIRECTORY, NO YEAR COMPONENT*/
%LET den = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/PAC/Data/Raw/Denominator;

/*BSF RAW DATA - ROOT DIRECTORY, NO YEAR COMPONENT*/
%LET bsf = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/PAC/Data/Raw/Denominator;

/*HHA RAW DATA - ROOT DIRECTORY, NO YEAR COMPONENT*/
%LET hha = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/PAC/Data/Raw/HHA;

/*OUTPATIENT RAW DATA - ROOT DIRECTORY, NO YEAR COMPONENT*/
%LET op = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/PAC/Data/Raw/Other;

/*XWALKED DATA FOR PRE-2005*/
%LET xw = /disk/agedisk3/medicare.work/goldman-DUA25731/DATA/Clean_Data;

/*MEASURE ANALYTIC FILE PRODUCTION LOCATION*/
LIBNAME dx_data  "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Processed";

/*LOCATION FOR FINAL MEASURE ANALYTIC FILES*/
LIBNAME An_files "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final";

/*LOCATION FOR FORMATTED COVERAGE DATASET AND FLAG_TRANSFERS DATASET*/
LIBNAME data_an  "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Processed";

/*INPUT DATA LOCATIONS*/
**********************************;
/*STAY DATASET LOCATION*/
LIBNAME data_sty "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Processed";
/*BENE AND COVERAGE DATASET LOCATION*/
LIBNAME data_ben "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Raw";
/*PART A INPATIENT AND OUTPATIENT DATASET LOCATION*/
LIBNAME data_pta "/sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Raw";
/*PART B DATASET LOCATION*/
LIBNAME data_ptb "";
**********************************************************************;



**********************************************************************
* INPUT DATASET NAMES
**********************************************************************;
%LET stay_dataset 		  = stay_dataset;
%LET stay_dataset_pac     = stay_dtaset_pac;
%LET bene_dataset 		  = bene_dataset;
%LET coverage_dataset 	  =;
%LET pta_in_base_dataset  = pta_in_base_dataset;
%LET pta_in_line_dataset  = pta_in_line_dataset;  /*ONLY USED IN STAY CREATION*/
%LET pta_out_base_dataset = pta_out_base_dataset;
%LET pta_out_line_dataset = pta_out_line_dataset;
%LET ptb_line_dataset 	  =;
**********************************************************************;
