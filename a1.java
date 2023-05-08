package com.covansys.jclarety.optionalservicecredit.app.internal.fabm;

/**
 *FABMOSC:
 *  
 *@author :
 */
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.covansys.jclarety.Comparator.QSortComparator;
import com.covansys.jclarety.InvestmentOption.app.api.be.BVIntAmt;
import com.covansys.jclarety.account.app.api.be.BEAcctTrans;
import com.covansys.jclarety.account.app.api.be.BECntrb;
import com.covansys.jclarety.account.app.api.be.BEFndgRqmt;
import com.covansys.jclarety.account.app.api.be.BEIntItem;
import com.covansys.jclarety.account.app.api.be.BEIntRateRef;
import com.covansys.jclarety.account.app.api.be.BEMbrAcct;
import com.covansys.jclarety.account.app.api.be.BEPyrl;
import com.covansys.jclarety.account.app.api.be.BESrvCrdtRef;
import com.covansys.jclarety.account.app.api.be.BETierRef;
import com.covansys.jclarety.account.app.api.be.BVFABMMbrAcctMaintCalcEndDtforStrtDtNSC;
import com.covansys.jclarety.account.app.api.be.BVFabmMbrAcctMaintCalcContrb;
import com.covansys.jclarety.account.app.api.be.BVFndgRqmt;
import com.covansys.jclarety.account.app.api.be.BVFndgRqmtCntrb;
import com.covansys.jclarety.account.app.api.be.BVMbrAcctPln;
import com.covansys.jclarety.account.app.api.be.BVPyrlDtl;
import com.covansys.jclarety.account.app.api.be.BVRfndTrans;
import com.covansys.jclarety.account.app.api.be.BVSrvCrdtCatgAmt;
import com.covansys.jclarety.account.app.api.be.BVSrvcCrdt;
import com.covansys.jclarety.account.app.api.be.BVTierHistDtls;
import com.covansys.jclarety.account.app.api.be.BVYrlyRfndTrans;
import com.covansys.jclarety.account.app.api.be.TPAcctTransId;
import com.covansys.jclarety.account.app.api.be.TpSrvStatHist;
import com.covansys.jclarety.account.app.api.be.tpfndgrqmt;
import com.covansys.jclarety.account.app.api.be.tpintitem;
import com.covansys.jclarety.account.app.api.be.tplateintpstgrpt;
import com.covansys.jclarety.account.app.internal.db.DBAcctTrans;
import com.covansys.jclarety.account.app.internal.db.DBCntrb;
import com.covansys.jclarety.account.app.internal.db.DBFndgRqmt;
import com.covansys.jclarety.account.app.internal.db.DBIntItem;
import com.covansys.jclarety.account.app.internal.db.DBIntRateRef;
import com.covansys.jclarety.account.app.internal.db.DBLateIntPstgRpt;
import com.covansys.jclarety.account.app.internal.db.DBMbrAcct;
import com.covansys.jclarety.account.app.internal.db.DBPyrl;
import com.covansys.jclarety.account.app.internal.db.DBSrvCrdtRef;
import com.covansys.jclarety.account.app.internal.fabm.FABMMbrAcctMaint;
import com.covansys.jclarety.account.app.internal.fabm.FABMPlanTier;
import com.covansys.jclarety.arch.app.api.BECmnValdn;
import com.covansys.jclarety.arch.app.base.BaseFABM;
import com.covansys.jclarety.arch.app.internal.batch.be.BEBusnDt;
import com.covansys.jclarety.arch.app.internal.batch.db.DBBusnDt;
import com.covansys.jclarety.arch.common.base.ArchitectureConstants;
import com.covansys.jclarety.arch.common.base.Assert;
import com.covansys.jclarety.arch.common.base.ClaretyException;
import com.covansys.jclarety.arch.common.base.DateUtility;
import com.covansys.jclarety.arch.common.base.IntervalData;
import com.covansys.jclarety.benefitcalculating.app.api.be.BEAnlSal;
import com.covansys.jclarety.benefitcalculating.app.api.be.BEBeneEstmtWage;
import com.covansys.jclarety.benefitcalculating.app.api.be.BEIRCMaxAmtRef;
import com.covansys.jclarety.benefitcalculating.app.api.be.BESalCapRateRef;
import com.covansys.jclarety.benefitcalculating.app.api.be.BVFabmGBEPrepareAnlSalNCalaAvgComp;
import com.covansys.jclarety.benefitcalculating.app.internal.db.DBBeneEstmtWage;
import com.covansys.jclarety.benefitcalculating.app.internal.db.DBIRCMaxAmtRef;
import com.covansys.jclarety.benefitcalculating.app.internal.db.DBSalCapRateRef;
import com.covansys.jclarety.benefitcalculating.app.internal.fabm.FABMGBE;
import com.covansys.jclarety.cachedlob.app.api.be.BEDocStatRef;
import com.covansys.jclarety.cachedlob.app.api.be.BESrvTypRef;
import com.covansys.jclarety.demographics.app.api.be.BEAddr;
import com.covansys.jclarety.demographics.app.api.be.BEMbr;
import com.covansys.jclarety.demographics.app.api.be.BEPart;
import com.covansys.jclarety.demographics.app.api.be.BEPrsn;
import com.covansys.jclarety.demographics.app.internal.db.DBAddr;
import com.covansys.jclarety.demographics.app.internal.db.DBMbr;
import com.covansys.jclarety.demographics.app.internal.db.DBPart;
import com.covansys.jclarety.demographics.app.internal.db.DBPrsn;
import com.covansys.jclarety.disbursements.app.api.be.BEOutgngPymt;
import com.covansys.jclarety.disbursements.app.api.be.BVChkDtl;
import com.covansys.jclarety.disbursements.app.internal.fabm.FABMCashDsbrs;
import com.covansys.jclarety.employment.app.api.be.BEEmptHist;
import com.covansys.jclarety.employment.app.api.be.BEFscYr;
import com.covansys.jclarety.employment.app.api.be.BEPstnHist;
import com.covansys.jclarety.employment.app.api.be.BVConstructFscDates;
import com.covansys.jclarety.employment.app.api.be.BVEmptTab;
import com.covansys.jclarety.employment.app.internal.db.DBEmptHist;
import com.covansys.jclarety.employment.app.internal.db.DBFscYr;
import com.covansys.jclarety.faglobal.constants.JClaretyConstants;
import com.covansys.jclarety.faglobal.util.RetirementPlanUtils;
import com.covansys.jclarety.generalledger.app.api.be.BEDocStatHist;
import com.covansys.jclarety.generalledger.app.api.be.BEFncAlloc;
import com.covansys.jclarety.generalledger.app.api.be.BEFncDoc;
import com.covansys.jclarety.generalledger.app.api.be.BEFncItem;
import com.covansys.jclarety.generalledger.app.api.be.BVFABMGLTransSaveFncDoc;
import com.covansys.jclarety.generalledger.app.api.be.BVFABMGLTransallocateItems;
import com.covansys.jclarety.generalledger.app.internal.db.DBDocStatHist;
import com.covansys.jclarety.generalledger.app.internal.db.DBDocStatRef;
import com.covansys.jclarety.generalledger.app.internal.db.DBFncAlloc;
import com.covansys.jclarety.generalledger.app.internal.db.DBFncItem;
import com.covansys.jclarety.generalledger.app.internal.fabm.FABMGLTrans;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEAgrmt;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEAgrmtMbrIntPstdLink;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BECostSchd;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEEmprChngDtl;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BELOA;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEMkupPstng;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEOSCRqst;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BEPymtInstr;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVAgrmt;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVAgrmtARLink;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVAgrmtSmry;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVCalculateFinanceCost;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFABMOSCDecideIfEmprCntrbIsOverridden;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFABMOSCSplitLeave;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFABMOSCVerifyPymtInstrNCash;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFABMOSCapplyChecks;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFabmOscPopulateRefundBuybackDetail;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVFabmOscSaveAgrmt;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVIRC415Lmts;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVOSCBal;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVOSCEligCheckMsg;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVOSCRqstInfo;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVPymtInstr;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVPyrlCntrbFndgRfndPybk;
import com.covansys.jclarety.optionalservicecredit.app.api.be.BVPyrlLOA;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBAgrmt;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBCostSchd;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBCubeFctr;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBEmprChngDtl;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBLOA;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBMkupPstng;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBOSCRqst;
import com.covansys.jclarety.optionalservicecredit.app.internal.db.DBPymtInstr;
import com.covansys.jclarety.organization.app.api.be.BEEmpr;
import com.covansys.jclarety.organization.app.api.be.BEOrg;
import com.covansys.jclarety.organization.app.api.be.BEPln;
import com.covansys.jclarety.organization.app.api.be.BERetSys;
import com.covansys.jclarety.organization.app.api.be.BVcheckSrvcAtRtrmt;
import com.covansys.jclarety.organization.app.internal.db.DBEmpr;
import com.covansys.jclarety.organization.app.internal.db.DBOrg;
import com.covansys.jclarety.organization.app.internal.db.DBPln;
import com.covansys.jclarety.organization.app.internal.db.DBRetSys;
import com.covansys.jclarety.organization.app.internal.fabm.FABMOrganization;
import com.covansys.jclarety.receipts.app.api.be.BVUnallocChks;
import com.covansys.jclarety.receipts.app.internal.fabm.FABMCashRcpts;
import com.covansys.jclarety.refunds.app.api.be.BERfndReq;
import com.covansys.jclarety.refunds.app.internal.db.DBRfndReq;
import com.covansys.jclarety.taxes.app.api.be.BVRetSys;
import com.covansys.jclarety.wageandcontribution.app.api.be.BEAcctRcvbl;
import com.covansys.jclarety.wageandcontribution.app.api.be.BEPlnRef;
import com.covansys.jclarety.wageandcontribution.app.api.be.BEWCAcctPybl;
import com.covansys.jclarety.wageandcontribution.app.api.be.BVUnallocCash;
import com.covansys.jclarety.wageandcontribution.app.internal.db.DBAcctRcvbl;
import com.covansys.jclarety.wageandcontribution.app.internal.db.DBMbrExmptnDtls;
import com.covansys.jclarety.wageandcontribution.app.internal.db.DBPlnRef;
import com.covansys.jclarety.wageandcontribution.app.internal.db.DBWCAcctPybl;
import com.covansys.jclarety.wageandcontribution.app.internal.fabm.FABMWageAndCntrb;
import com.covansys.jclarety.workflow.app.api.be.BEActy;
import com.covansys.jclarety.workflow.app.api.be.BEPrcs;
import com.covansys.jclarety.workflow.app.internal.db.DBPrcs;

public class FABMOSC extends BaseFABM {		
	private DBPln dbpln;
	private DBLateIntPstgRpt dbLateIntPstgRpt;
	private DBIntRateRef dbIntRateRef;
	private BEFncDoc befncdoc;
	private BEMbr bembr;
	private BEPln bepln;	
	private DBAcctTrans dbaccttrans;
	private DBAgrmt dbagrmt;	
	private DBCntrb dbcntrb;
	private DBCostSchd dbcostschd;
	private DBDocStatHist dbdocstathist;
	private DBDocStatRef dbdocstatref;
	private DBEmpr dbempr;	
	private DBEmptHist dbempthist;
	private DBFncAlloc dbfncalloc;
	private DBFncItem dbfncitem;
	private DBFndgRqmt dbfndgrqmt;
	private DBFscYr dbfscyr;
	private DBIntItem dbintitem;
	private DBMbr dbmbr;
	private DBMbrAcct dbmbracct;
	private DBOrg dborg;
	private DBOSCRqst dboscrqst;
	private DBPart dbpart;
	private DBPyrl dbpyrl;
	private DBSrvCrdtRef dbsrvcrdtref;
	private DBPlnRef dBPlnRef;
	private FABMGLTrans fabmgltrans;
	private FABMMbrAcctMaint fabmmbracctmaint;
	private FABMOrganization fabmorganization;
	private DBIRCMaxAmtRef dbircmaxamtref;
	private DBPymtInstr dBPymtInstr;
	private DBLOA dBLOA;
	private DBPrcs dbPrcs;
	private DBAddr dbaddr;
	private DBPymtInstr dbPymtInstr;
	private DBRetSys dbRetSys;
	private DBWCAcctPybl dbWCAcctPybl;
	private FABMWageAndCntrb fabmwageandcntrb;
	private FABMCashDsbrs fabmcashdsbrs;
	private FABMCashRcpts fabmcashrcpts;
	private static final int OSC_TOLERANCE_AMT = 10;
	private DBRfndReq dbRfndReq;
	private DBMkupPstng dbMkupPstng;
	private DBEmprChngDtl dbEmprChngDtl;
	private DBBusnDt dBBusnDt;	//1A@PIR5564-vgp-01102012
	private DBAcctTrans dbAcctTrans;//A@6481

	/*********************************************************
	 * Method Name : SplitLeave()
	 *
	 * Input Parameters	:
	 *	P_BELOAOrig : BE_LOA
	 *	P_BEPyrl : BE_Pyrl
	 *	P_BEPln : BE_Pln
	 *	
	 * Output Parameters	:
	 *	P_BEPyrl : BE_Pyrl
	 *	P_aoBELOA : Array of BE_LOA
	 *	P_IsSrvCrdtAccurate : boolean
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method accepts the original leave, then decides to split the original 
	 *	leave by the user entered service credit quantity.
	 *	This method accepts the original leave, and the overridden service credit 
	 *	quantity to get the end date for the service credit quantity. 
	 *	Once the end date is known it splits the original leave record in two records
	 *	1) The first record will have the start date of the original record and the end 
	 *	date of the calculate end date
	 *	2) the second record will have the start date as the calculate end date + 1 day 
	 *	and the end date the original end date of the leave. 
	 *	3) The rest of the attributes value remains the same
	 *
	 * Change History
	 * =================================================================
	 *		CCR#		Updated		Updated 
	 * Ver	PIR#		By			On			Description
	 * ====	====		========	==========	=======================
	 * NE.01			nyadav	    07/29/2008	Migration From forte
	 *******************************************************************/
	public BVFABMOSCSplitLeave splitLeave(BELOA pBELOAOrig, BEPyrl pBEPyrl,	BEPln pBEPln)throws ClaretyException{
		startBenchMark("splitLeave", FABM_METHODENTRY);
		addTraceMessage("splitLeave",FABM_METHODENTRY);

		BVFABMOSCSplitLeave bvFABMOSCSplitLeave = new BVFABMOSCSplitLeave();

		BVMbrAcctPln bvMbrAcctPln = null;
		List paoBELOA = null;
		boolean pIsSrvCrdtAccurate = false;
		//Get the member account
		bvMbrAcctPln = dbmbracct.bVReadMbrAcctPlnByMbrAcctId(pBEPyrl.acct_id, null); 
		if(errorsPresent()){
			return null;
		}

		BELOA cloneBELOA = (BELOA)pBELOAOrig.clone();
		cloneBELOA.opt_lck_ctl = ZERO_INTEGER;
		cloneBELOA.loa_id = ZERO_INTEGER;

		IntervalData tmpIntData = new IntervalData();
		tmpIntData.setUnit(0, 0, 1, 0, 0, 0, 0);
		Date tmpEndDt  = getTimeMgr().getCurrentDateOnly();
		if(bvMbrAcctPln != null){
			BEMbrAcct beMbrAcct = (BEMbrAcct)bvMbrAcctPln;
			//Pass in the BE_Pyrl with SC & Start date populated to get the End Date calculated
			BVFABMMbrAcctMaintCalcEndDtforStrtDtNSC bvFABMMbrAcctMaintCalcEndDtforStrtDtNSC = fabmmbracctmaint.calcEndDtforStrtDtNSC(pBEPyrl, pBEPln, beMbrAcct);
			if(errorsPresent()){
				return null;
			}
			pIsSrvCrdtAccurate = bvFABMMbrAcctMaintCalcEndDtforStrtDtNSC.pIsSrvCrdtAccurate;
			pBEPyrl = bvFABMMbrAcctMaintCalcEndDtforStrtDtNSC.bePyrl;
		}
		if(!pIsSrvCrdtAccurate){
			//Do nothing here, calling method needs to use the IsSrvCrdtAccurate o/p parameter and take the 
			//BE_Pyrl populated with end date to make the user recalculate the SC based on the new end date now		
		}else{		
			//Service credit entered on the tab is a round/accurate value enabling calculation of 
			//an accurate end date. so, split the LOA now based on the new end date on BE_Pyrl
			tmpEndDt = pBEPyrl.end_dt;		
			//Set end date of Orig LOA to new end date on BE_Pyrl, this is the 1st part of the split
			pBELOAOrig.end_dt = tmpEndDt;		
			//Add one day to the new end date so that it can used to set the start date of the 2nd split
			tmpEndDt = DateUtility.add(tmpEndDt, tmpIntData);		
			//Set the start date of the 2nd part of split to new end dt + one day
			//The original end date is intact in this cloned BELOA
			cloneBELOA.strt_dt = tmpEndDt;		
			//Set the o/p parameter, array of BE_LOA to contain both the Original LOA (1st part of the split)
			// & the cloned LOA (2nd part of the split)
			paoBELOA = new ArrayList();
			paoBELOA.add(pBELOAOrig);
			paoBELOA.add(cloneBELOA);
		}
		bvFABMOSCSplitLeave.paoBELOA = paoBELOA;
		bvFABMOSCSplitLeave.pBEPyrl = pBEPyrl;
		bvFABMOSCSplitLeave.pIsSrvCrdtAccurate = pIsSrvCrdtAccurate;
		addTraceMessage("splitLeave", FABM_METHODEXIT);
		takeBenchMark("splitLeave", FABM_METHODEXIT);
		return bvFABMOSCSplitLeave;
	}
	/******************************************************************************************
	 * Method Name : checkIRC415Lmts()
	 *
	 * Input Parameters	:
	 *	P_BEPln : BE_Pln
	 *	P_EffDt : DateTimeData
	 *	P_BEMbr : BE_Mbr
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *	P_BEOSCRqst : BE_OSC_Rqst = nil
	 *	P_BVAgrmt : BV_Agrmt = nil
	 *	
	 * Output Parameters	:
	 *	P_BVIRC415Lmts : BV_IRC415Lmts
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method is called by TabRequest & TabAgreement to check for the IRC415 rule	
	 *	which says, the total post tax contribution that can be accumulated 
	 *	in a fiscal year, cannot exceed lesser of $40,000.00 (value specified in the 
	 *	be_irc_max_amt_ref table) or 100% of the gross salary
	 *
	 * Change History
	 * ============================================================================================
	 *		CCR#		Updated		Updated 
	 * Ver	PIR#		By			On			Description
	 * ====	====		========	==========	=======================
	 * NE.01			nyadav  	 07/29/2008	Initial Migration From Forte
	 * P.02	NPRIS-5570	MKolm		10/19/2015	Changed from fiscal year to Calendar year for 415 limit
	 *********************************************************************************************/
	public BVIRC415Lmts checkIRC415Lmts(BEPln pBEPln, Date pEffDt, BEMbr pBEMbr, BEMbrAcct pBEMbrAcct, 
			BEOSCRqst pBEOSCRqst, BVAgrmt pBVAgrmt)throws ClaretyException{
		addTraceMessage("checkIRC415Lmts", FABM_METHODEXIT);
		startBenchMark("checkIRC415Lmts", FABM_METHODEXIT);
		BVIRC415Lmts bvIRC415Lmts = new BVIRC415Lmts();

		BigDecimal base415Amt;
		List tmpaoBVAgrmt = null ;
		BigDecimal fYAmt;
		BigDecimal fYAmtSum = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal postTaxAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal grandTotal = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal potentialAgrmtAmt = JClaretyConstants.ZERO_BIGDECIMAL;

		//5D@P.02
//		//Find out fiscal year of the P_EffDt
//		int fscYr = dbfscyr.calculateFiscalYearNr(pBEPln, pEffDt);
//		if(errorsPresent()){
//			return null;
//		}
		
		
		//2A@P.02
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(pEffDt);

		//Call GetIRC415AmtsRem to populate current year projected wage amount, last year wage amt, irc index max value		
		bvIRC415Lmts =this.getIRC415Amts(pBEPln, pEffDt, pBEMbrAcct);
		if(errorsPresent()){
			return null;
		}
		if(bvIRC415Lmts!= null){
			//Read all agreements for the member
			tmpaoBVAgrmt = dbagrmt.readByMbrNPlnNOrgNStatGrp(pBEMbr, pBEPln, null, null, pEffDt); 
			//stat_grp_cd = All by default
			if(errorsPresent()){
				return null;
			}
			if(tmpaoBVAgrmt!= null){
				//For i in TmpaoBVAgrmt.items to 1 by -1 do
				for(int i=tmpaoBVAgrmt.size() -1 ; i>=0;i--){
					BVAgrmt bvAgrmt = (BVAgrmt)tmpaoBVAgrmt.get(i);
					if(bvAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)||
							bvAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)){
						continue;
					}
					if(pBVAgrmt!= null){					
						if(bvAgrmt.agrmt.agrmt_id == pBVAgrmt.agrmt.agrmt_id){
							//Drop the agreement if passed from TabAgrmt, because we want to use the
							//updated Agrmt passed from the tab, and not what's from the DB						
							//TmpaoBVAgrmt.Deleterow(i);						
							continue;
						}
					}
//					//Get the Fiscal year allocation amount for the agreement
//					fYAmt = this.getFscYrlyAllocInfo(bvAgrmt, pBEPln, fscYr); //1D@P.02
					
					//Get the Calendar year allocation amount for the agreement
					fYAmt = this.getYearlyAllocInfo(bvAgrmt, pBEPln, calendar.get(Calendar.YEAR)); //1M@P.02	
					if(errorsPresent()){
						return null;
					}
					//Keep adding the Fiscal Year amount
					fYAmtSum = fYAmtSum.add(fYAmt);
					if(fYAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL)!= JClaretyConstants.ZERO_INTEGER ||pBVAgrmt!= null){
						bvIRC415Lmts.total_cost_agrmts = bvIRC415Lmts.total_cost_agrmts.add(bvAgrmt.agrmt.tot_cost);
					}


//					Added the agreements which are in agreement and the payment is post Tax are not consider in the above calculation
//					Since Agreement that are in status 'A'  have not yet received the payments but the agreements are in potential 
//					to receive the money we need to add these agreements. 
//					This is not valid for the batch, that is why is added separety 

					if(bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_A)){
						for(int j=0; j<bvAgrmt.aobepymtinstr.size();j++){
							BEPymtInstr row =(BEPymtInstr)bvAgrmt.aobepymtinstr.get(j);
							if(row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)){
								potentialAgrmtAmt = potentialAgrmtAmt.add(row.pymt_amt);
							}else if(row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_INST)){
								potentialAgrmtAmt = potentialAgrmtAmt.add(row.pymt_amt.multiply(new BigDecimal(row.nbr_of_pymts)));

							}
						}
					}
				}
				bvIRC415Lmts.tot_post_tax_amt  = fYAmtSum;
			}
			//////////////////////////////////////////////////////////////////////////////
			//Uptil now we have common code for Tab Request/Tab Agreement/IRC 415 Batch
			//Mow we are going to add some extra code to take care of request or agreement 
			//that gets passed in if this method is called from the tabs
			/////////////////////////////////////////////////////////////////////////////
			//Set various types of error messages for the Tabs
			if(pBEOSCRqst != null ){
				//This method has been called from TabRequest
				//Add Also the Agreement Amount in "A" status of type post tax				
				grandTotal = pBEOSCRqst.base_cost.add(fYAmtSum.add(potentialAgrmtAmt));	
				if(pBEOSCRqst.base_cost.compareTo(bvIRC415Lmts.irc_415_index_lmt_amt)>JClaretyConstants.ZERO_INTEGER){
					bvIRC415Lmts.error_typ_1 = JClaretyConstants.OVERRIDE_YES;
				}else{
					if(grandTotal.compareTo(bvIRC415Lmts.irc_415_index_lmt_amt)>JClaretyConstants.ZERO_INTEGER){
						bvIRC415Lmts.error_typ_1 = JClaretyConstants.OVERRIDE_YES;
					}
				}				
				bvIRC415Lmts.tot_post_tax_amt = grandTotal;
				bvIRC415Lmts.total_cost_agrmts = bvIRC415Lmts.total_cost_agrmts.add(pBEOSCRqst.base_cost);
			}else if(pBVAgrmt !=null){
				//This method has been called from TabAgreement
				//Agrmts in status Pending or Rejected or Audited or Agreement
				if(pBVAgrmt.agrmt_stat_cd .trim().equals(JClaretyConstants.AGRMT_STAT_P)||
						pBVAgrmt.agrmt_stat_cd .trim().equals(JClaretyConstants.AGRMT_STAT_RA)||
						pBVAgrmt.agrmt_stat_cd .trim().equals(JClaretyConstants.AGRMT_STAT_AD)||
						pBVAgrmt.agrmt_stat_cd .trim().equals(JClaretyConstants.AGRMT_STAT_A)||
						pBVAgrmt.agrmt_stat_cd .trim().equals(JClaretyConstants.AGRMT_STAT_RE)){
					for(int i=0;i<pBVAgrmt.aobepymtinstr.size();i++){
						BEPymtInstr row = (BEPymtInstr)pBVAgrmt.aobepymtinstr.get(i);
						if(!row.pre_tax_in){
							//keep track of post tax amount that the member has promised to pay
							if(row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)){
								postTaxAmt = postTaxAmt.add(row.pymt_amt);	
							}else if(row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_INST)){
								postTaxAmt = postTaxAmt.add(row.pymt_amt.multiply(new BigDecimal(row.nbr_of_pymts)));
							}
						}
					}
					//Add Also the Agreement Amount in "A" status of type post tax				
					//GrandTotal.value = PostTaxAmt.value + FYAmtSum.value;
					grandTotal = postTaxAmt.add(fYAmtSum.add(potentialAgrmtAmt));
					if(postTaxAmt.compareTo(bvIRC415Lmts.irc_415_index_lmt_amt)>JClaretyConstants.ZERO_INTEGER){
						bvIRC415Lmts.error_typ_1 = JClaretyConstants.OVERRIDE_NO;
					}else{
						if(grandTotal.compareTo(bvIRC415Lmts.irc_415_index_lmt_amt)>JClaretyConstants.ZERO_INTEGER){
							bvIRC415Lmts.error_typ_1 = JClaretyConstants.OVERRIDE_NO;
						}
					}
				}
				bvIRC415Lmts.tot_post_tax_amt = grandTotal;
				bvIRC415Lmts.total_cost_agrmts = bvIRC415Lmts.total_cost_agrmts.add(pBVAgrmt.agrmt.tot_cost);
			}
			if(grandTotal.compareTo(JClaretyConstants.ZERO_BIGDECIMAL)>JClaretyConstants.ZERO_INTEGER){
				if(grandTotal.compareTo(bvIRC415Lmts.last_yr_tot_wage_amt)>JClaretyConstants.ZERO_INTEGER){
					bvIRC415Lmts.error_typ_2 = JClaretyConstants.OVERRIDE_YES;
				}
				if(grandTotal.compareTo(bvIRC415Lmts.currnt_yr_prjctd_wage_amt)>JClaretyConstants.ZERO_INTEGER){
					bvIRC415Lmts.error_typ_3 = JClaretyConstants.OVERRIDE_YES;
				}
			}		        
		}

		addTraceMessage("checkIRC415Lmts", FABM_METHODEXIT);
		takeBenchMark("checkIRC415Lmts", FABM_METHODEXIT);
		return bvIRC415Lmts;
	}
	/******************************************************************************************
	 * Method Name : GetIRC415AmtsRem()
	 *
	 * Input Parameters	:
	 *	P_BEPln : BE_Pln
	 *	P_EffDt : DateTimeData
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *
	 * Output Parameters	:
	 *	P_BVIRC415Lmts : BV_IRC415Lmts
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	The IRC 415 rule says, the total post tax contribution that can be accumulated 
	 *	in a fiscal year, cannot exceed lesser of $40,000.00 (value specified in the 
	 *	be_irc_max_amt_ref table) or 100% of the gross salary
	 *	This method returns 
	 *	1) the IRC stipulated max. value
	 *	2) gross salary of the fiscal year derived from P_EffDt
	 *	3) BaseAmt = 1) or 2) above here whichever is lesser
	 *	4) 90% of last fy salary
	 *	5) projected salary of the current year	
	 *
	 * Change History
	 * ============================================================================================
	 *		CCR#		Updated		Updated 
	 * Ver	PIR#		By			On			Description
	 * ====	====		========	==========	=======================
	 * NE.01			nyadav  	 07/30/2008	Initial Migration From Forte
	 * NE.02			jmisra  	 10/20/2008	Code modified after review.
	 * P.03  5570  		MKolm 		 05/30/2014 Changed from fiscal year to calendar year for 415 limit
	 *********************************************************************************************/
	public BVIRC415Lmts getIRC415Amts(BEPln pBEPln,	Date pEffDt, BEMbrAcct pBEMbrAcct)	throws ClaretyException {
		startBenchMark("getIRC415Amts", FABM_METHODENTRY);
		addTraceMessage("getIRC415Amts", FABM_METHODENTRY);

		BVIRC415Lmts p_bvIRC415Lmts= new BVIRC415Lmts();
		BEIRCMaxAmtRef beIRCMaxAmtRef = null;
		BigDecimal currFYWagesSum = null;
		BigDecimal prevFYWagesSum = null;
		BigDecimal projWagSum = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoBEPyrl= null;
		BEPstnHist bePstnHist = new BEPstnHist();
		BVEmptTab bvEmptTab = null;
		BEEmpr beEmpr = null;
//		int fscYr = 0;	//1D@P.03
		//NE.02 - Begin
		//2D@P.03
//		Date fscStrtdt = new Date();
//		Date fscEnddt = new Date();
		Date tmpStrtDt  = new Date();
		//NE.02 - End
		IntervalData  intData = new IntervalData();
		intData.setUnit(0, 0, 1, 0, 0, 0, 0);		
		//Get the IRC stipulated value
		beIRCMaxAmtRef  = dbircmaxamtref.readByPlnNEffDt(pBEPln, pEffDt);
		if(errorsPresent()){
			return null;
		}
		//3D@P.03
//		//Find out fiscal year of the P_EffDt
//		fscYr	= dbfscyr.calculateFiscalYearNr(pBEPln, pEffDt);
//		if(errorsPresent()){
//			return null;
//		}
		
		//2A@P.03
		//Get the gross salary of the calendar year derived from P_EffDt 
		Calendar cal = Calendar.getInstance(); 
		cal.setTime(pEffDt);
		currFYWagesSum = dbpyrl.sumWagesForCalYr(pBEMbrAcct.acct_id, cal.get(Calendar.YEAR)); //1M@P.03
		if(errorsPresent()){
			return null;
		}
		if(beIRCMaxAmtRef != null){
			if(beIRCMaxAmtRef.osc_max_amt!= null){
				p_bvIRC415Lmts.irc_415_index_lmt_amt = beIRCMaxAmtRef.osc_max_amt;
			}else{
				addErrorMsg(1037, new String []{"OSC IRC 415 limitation index amount missing"});
				return null;
			}
		}
//		BaseAmt = IRC stipulated value or gross salary of fiscal year, whichever is lesser
		if(beIRCMaxAmtRef != null){
			if(currFYWagesSum != null){
				p_bvIRC415Lmts.currnt_yr_wage = currFYWagesSum;
				if(beIRCMaxAmtRef.osc_max_amt.compareTo(currFYWagesSum)>JClaretyConstants.ZERO_INTEGER){
					p_bvIRC415Lmts.base_amt = currFYWagesSum;
				}else{
					p_bvIRC415Lmts.base_amt = beIRCMaxAmtRef.osc_max_amt;
				}
			}else{
				p_bvIRC415Lmts.base_amt = beIRCMaxAmtRef.osc_max_amt;
			}
		}
//		Find out last Calendar salary - send in Calendar - 1yr
		prevFYWagesSum = dbpyrl.sumWagesForCalYr(pBEMbrAcct.acct_id, cal.get(Calendar.YEAR) - 1); //1M@P.03
		if(errorsPresent()){
			return null;
		}
		if(prevFYWagesSum != null){
			//Find out 90% last FY salary
			prevFYWagesSum = prevFYWagesSum.multiply(new BigDecimal(0.90));
			p_bvIRC415Lmts.last_yr_tot_wage_amt = prevFYWagesSum;
		}
		//D@P.03 Start
//		Find out fiscal year start & end dates				
//		if(fscYr > JClaretyConstants.ZERO_INTEGER){
//			BVConstructFscDates bvConstructFscDates = dbfscyr.constructFscDates(pBEPln, fscYr, pEffDt);			
//			if(errorsPresent()){
//				return null;
//			}
//			fscEnddt = bvConstructFscDates.fscEndDt;
//			fscStrtdt = bvConstructFscDates.fscStrtDt;
//		}
		//D@P.03 End
		
		//5A@P.03 - Setting calendar year based on passed in effective date
		int calYr = cal.get(Calendar.YEAR);
		cal.set(calYr, Calendar.JANUARY, JClaretyConstants.ONE_INTEGER);
		Date calStrtDt = cal.getTime();
		cal.set(calYr, Calendar.DECEMBER, JClaretyConstants.THIRTY_ONE_INTEGER);
		Date calEndDt = cal.getTime();
		
//		Get the latest posting in the calendar year of the Eff date
//		aoBEPyrl = dbpyrl.readForLatestPstngByYear(pBEMbrAcct, fscYr); //1D@P.03
		aoBEPyrl = dbpyrl.readForLatestPstngByCalYear(pBEMbrAcct, cal.get(Calendar.YEAR)); //1A@P.03
		if(errorsPresent()){
			return null;
		}
		if(aoBEPyrl != null && aoBEPyrl.size()>JClaretyConstants.ZERO_INTEGER){
			for(int i =0;i<aoBEPyrl.size();i++){
				BEPyrl row = (BEPyrl)aoBEPyrl.get(i);
				//multiple rows in this array only if member has been reported by more than one employer
				bePstnHist.pstn_hist_id = row.pstn_hist_id;
				bvEmptTab = dbempthist.readEmptByPstnId(pBEMbrAcct, bePstnHist);
				if(errorsPresent()){
					return null;
				}
				//Read employer using org_id													
				if(bvEmptTab != null){
					beEmpr = dbempr.readByOrgId(bvEmptTab.org_id);
					if(errorsPresent()){
						return null;
					}
				}
				if(beEmpr != null){
					List tmpaoBEPyrl = null;
					tmpStrtDt = row.end_dt;
					//find no. of payroll period between last posting pyrl date & end of calendar year //1M@P.03
					tmpStrtDt = DateUtility.add(tmpStrtDt, intData);
					//Read on FABMORG to get all aobepyrl
//					tmpaoBEPyrl = fabmorganization.buildPyrlForEmpr(beEmpr, tmpStrtDt, fscEnddt); 
					tmpaoBEPyrl = fabmorganization.buildPyrlForEmpr(beEmpr, tmpStrtDt, calEndDt); //1A@P.03
					if(errorsPresent()){
						return null;
					}
					if(tmpaoBEPyrl != null){
						projWagSum = projWagSum.add(row.wag_amt.multiply(new BigDecimal(tmpaoBEPyrl.size())));
					}
				}
			}
			if(projWagSum != null && currFYWagesSum != null){
				currFYWagesSum = currFYWagesSum.add(projWagSum);
				//90% of Current fiscal year projected wages
				p_bvIRC415Lmts.currnt_yr_prjctd_wage_amt = currFYWagesSum.multiply(new BigDecimal(0.90));
			}
		}
		addTraceMessage("getIRC415Amts", FABM_METHODEXIT);
		takeBenchMark("getIRC415Amts", FABM_METHODEXIT);
		return p_bvIRC415Lmts;
	}
	/************************************************************************************
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					skumar4     03/31/2008  Initial migration from FORTE
	 *************************************************************************************/
	public void applyRefundRepay(BEPln pBEPln,BEMbrAcct pBEMbrAcct, BERfndReq pBERfndReq,BigDecimal pPretax, BigDecimal pPostTax,
			Date pEffDt) throws ClaretyException {
		startBenchMark("applyRefundRepay", FABM_METHODENTRY);
		addTraceMessage("applyRefundRepay",FABM_METHODENTRY);
		List aoBEAcctTrans = null;
		List aoBVPyrlDtl = null;
		List aoBVPyrlDtlForPrch = new ArrayList();
		List tempaoBECostSchd = null;
		BECostSchd bECostSchd = null;
		BESrvCrdtRef bESrvCrdtRef = null;
		BEAcctTrans bEAcctTrans = null;
		BECostSchd tempBECostSchd = null;
		BVPyrlDtl bVPyrlDtl = null;
		int index = 0;
		int sizeOfAl = 0;
		this.updateRefundCostScheduleForMbrAcct(pBEMbrAcct,0);
		if (errorsPresent()) {
			return;
		}

		tempaoBECostSchd = dbcostschd.readByMbrAcctNSrvTyp(pBEMbrAcct,JClaretyConstants.SRV_CRDT_TYP_CD_BRFD);
		if (errorsPresent()) {
			return;
		}
		if (tempaoBECostSchd != null && tempaoBECostSchd.size() > JClaretyConstants.ZERO_INTEGER) {
			tempBECostSchd = (BECostSchd) tempaoBECostSchd.get(0);
			if (tempBECostSchd != null) {
				bECostSchd = tempBECostSchd;
				bESrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNMbrAcct(pBEMbrAcct,JClaretyConstants.SRV_CRDT_TYP_CD_BRFD,pEffDt);
				if (errorsPresent()) {
					return;
				}
			}
		}


		aoBEAcctTrans = dbaccttrans.readByRfndReq(pBERfndReq, pBEMbrAcct);
		if (errorsPresent()) {
			return;
		}
		if ( aoBEAcctTrans != null && aoBEAcctTrans.size() > JClaretyConstants.ZERO_INTEGER) {
			sizeOfAl = aoBEAcctTrans.size();
			for (index = 0; index < sizeOfAl; index++) {
				bEAcctTrans = (BEAcctTrans) aoBEAcctTrans.get(index);
				aoBVPyrlDtl = dbfndgrqmt.readBuybackPyrlDtlByAcctTrans(bEAcctTrans,pBEMbrAcct.acct_id, 0,0, 0);
				if (errorsPresent()) {
					return;
				}

				if (aoBVPyrlDtl != null
						&& aoBVPyrlDtl.size() > JClaretyConstants.ZERO_INTEGER) {
					int size = aoBVPyrlDtl.size();
					for (int i = 0; i < size; i++) {
						bVPyrlDtl = (BVPyrlDtl) aoBVPyrlDtl.get(i);
						aoBVPyrlDtlForPrch.add(bVPyrlDtl);
					}
				}

			}
			if ( aoBVPyrlDtlForPrch.size() > JClaretyConstants.ZERO_INTEGER) {
				bEAcctTrans = new BEAcctTrans();
				bEAcctTrans.acct_id = pBEMbrAcct.acct_id;
				bEAcctTrans.acct_trans_typ_cd = bESrvCrdtRef.acct_trans_typ_cd.trim();
				int tempInt = bECostSchd.cost_schd_id;
				bEAcctTrans.cost_schd_id = tempInt;
				bEAcctTrans.eff_dt = pEffDt;
				bEAcctTrans.trans_dt = pEffDt;
				bEAcctTrans.fscl_yr_end_nr = 2000;
				bEAcctTrans.fscl_yr_strt_nr = 2000; // years will be fiscal year of trans_dt.
				// but they must be set to something to get through the validations
				// done the by the TransOSCPurchase before it calls TransOSCPurchaseRem
				bEAcctTrans.hide_trans_in = false;
				bEAcctTrans.org_id = 0;
				BVPyrlDtl bVPyrlDtl2 = null;
				int size1 = aoBVPyrlDtlForPrch.size();
				for (int j = 0; j < size1; j++) {
					bVPyrlDtl2 = (BVPyrlDtl) aoBVPyrlDtlForPrch.get(j);
					bVPyrlDtl2.befndgrqmt.tot_rqd_amt = bVPyrlDtl2.befndgrqmt.pre_tax_amt.add(bVPyrlDtl2.befndgrqmt.post_tax_amt);
					bVPyrlDtl2.befndgrqmt.osc_rqd_int_amt = bVPyrlDtl2.befndgrqmt.osc_int_pre_tax_amt.add(bVPyrlDtl2.befndgrqmt.osc_int_post_tax_amt);
				}
				bEAcctTrans = fabmmbracctmaint.transOSCPurchase(pBEPln,pBEMbrAcct,bEAcctTrans,JClaretyConstants.CNTRB_STAT_CD_PSTD,
						aoBVPyrlDtlForPrch,bESrvCrdtRef.schd_typ_cd.trim(),pPretax, pPostTax,false, 0, null, false,false);
				if (errorsPresent()) {
					return;
				}
			}
			else {
				String params[] = {"Money could not be applied back to the account"};
				addErrorMsg(1037, params);
			}
		}

		addTraceMessage("applyRefundRepay", FABM_METHODEXIT);
		takeBenchMark("applyRefundRepay", FABM_METHODEXIT);
	}
	/*********************************************************
	 * Method Name : updateRefundCostScheduleForMbrAcct()
	 *
	 *
	 * Purpose :
	 *       One refund-based cost schedule exists per member account.  
	 *       This function finds the existing cost schedule or creates a new one.  
	 *       then it passes it to FABM_MAM, which checks for any new refund transactions, 
	 *       and builds for them the fndg_rqmts for the payback and attaches them to 
	 *       the cost schedule
	 *
	 * @param BEMbrAcct pBEMbrAcct,int pMergeSSN
	 * @return List of tp_fndg_rqmt
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					skumar4     03/31/2008  Initial migration from FORTE
	 * P.01    78778            Lashmi      03/11/2009  corrected conversion issue to match PIONEER   
	 *************************************************************************************/
	public List updateRefundCostScheduleForMbrAcct(BEMbrAcct pBEMbrAcct, int pMergeSSN)throws ClaretyException {
		startBenchMark("updateRefundCostScheduleForMbrAcct",FABM_METHODENTRY);
		addTraceMessage("updateRefundCostScheduleForMbrAcct",FABM_METHODENTRY);
		//output
		List paoTPFndgRqmt = null;
		List aoRfndTrans = null;
		List aoBECostSchd = null;
		List aoBEIntItem = null;
		List aoRfndFndgRqmt = null;//array of BV_PyrlCntrbFndgRfndPybk; fndg rqmts of single refund
		BECostSchd bECostSchd = null;
		int sizeOfAl = 0;
		int index = 0;
		BEAcctTrans bEAcctTrans = null;
		//This method returns refund transactions ordered by effective date 
		//This means that the first refund in time taken by the member is the first in the array.
		//This is desirable for processing the funding requirements of the method.
		aoRfndTrans = dbaccttrans.readRfndTransByMbrAcct(pBEMbrAcct, pMergeSSN);
		if (errorsPresent()) {
			return null;
		}

		if (aoRfndTrans != null && aoRfndTrans.size() > JClaretyConstants.ZERO_INTEGER) {
			//Member has taken refund. Read the refund buyback cost schedules for the member
			aoBECostSchd = dbcostschd.readByMbrAcctNSrvTyp(pBEMbrAcct,JClaretyConstants.SRV_TYP_CD_BRFD);
			if (errorsPresent()) {
				return null;
			}

			if (aoBECostSchd == null) {//Refund payback cost schedule doesn't exist
				//Build the cost schedule
				bECostSchd = new BECostSchd();
				bECostSchd.eff_dt = getTimeMgr().getCurrentDateOnly();
				bECostSchd.srv_crdt_typ_cd = JClaretyConstants.SRV_TYP_CD_BRFD;
				bECostSchd.acct_id = pBEMbrAcct.acct_id;

				bECostSchd = dbcostschd.save(bECostSchd);
				if (errorsPresent()) {
					return null;
				}

			}
			else if (aoBECostSchd.size() == JClaretyConstants.ONE_INTEGER) {
				bECostSchd = (BECostSchd) aoBECostSchd.get(0);
			}
			else {
				//Error, more than one cost schedule exists
				String params[] = {""};
				addErrorMsg(52, params);
				return null;
			}
		}
		if (aoRfndTrans != null) {
			sizeOfAl = aoRfndTrans.size();
			for (index = 0; index < sizeOfAl; index++) {
				bEAcctTrans = (BEAcctTrans) aoRfndTrans.get(index);
				if (errorsPresent()) {
					break;
				}
				//If the refund transaction is actually a reversal transaction or has been reversed, then skip it.
				if (bEAcctTrans.rvrs_trans_id != JClaretyConstants.ZERO_INTEGER ||
						bEAcctTrans.orig_trans_id != JClaretyConstants.ZERO_INTEGER) {
					continue;
				}
				//If the refund transaction is already associated with the cost schedule. 
				if (bEAcctTrans.getCostSchdId() != JClaretyConstants.ZERO_INTEGER) {
					continue;
				}
				//Do the processing for creating new fndg_rqmts.
				//associate the refund transaction with the cost schedule.
				bEAcctTrans = dbaccttrans.save(bEAcctTrans,pBEMbrAcct, bECostSchd, null, 0);//updates be_acct_trans, so P_mergeSSN not passed
				if (errorsPresent()) {
					return null;
				}
				aoRfndFndgRqmt = dbfndgrqmt.readRfndFndgRqmtByAcctTrans(bEAcctTrans, pMergeSSN);
				if (errorsPresent()) {
					return null;
				}
				//Int_Items are read by Rfnd_Trans even if there are no fndg_Rqmt for that Rfnd_Trans
				aoBEIntItem = dbintitem.readByAcctTrans(bEAcctTrans,pMergeSSN);				
				if (errorsPresent()) {
					break;
				}
				//If the next error condition were true, it would have been caught in the earlier loop.
				//We need to check this again here because we do the calculation only if there are any
				//refund type funding requirements.
				//If there are no Fndg_Rqmt for all of the Rfnd_Trans and if there are Int_Items then it is an error.
				//jaspinder?
				if (aoRfndFndgRqmt == null || aoRfndFndgRqmt.size() == JClaretyConstants.ZERO_INTEGER) {
					if (aoBEIntItem != null && aoBEIntItem.size() > JClaretyConstants.ZERO_INTEGER) {
						String params[] = {""};
						addErrorMsg(449, params);
						return null;
					}
				}
				List aoTPFndgRqmt = null;
				if (aoRfndFndgRqmt != null && aoRfndFndgRqmt.size() > JClaretyConstants.ZERO_INTEGER) {
					BVPyrlCntrbFndgRfndPybk bVPyrlCntrbFndgRfndPybk = null;
					tpfndgrqmt tpfndgrqmt1 = null;
					aoTPFndgRqmt = new ArrayList();
					int size = aoRfndFndgRqmt.size();
					for (int j = 0; j < size; j++) {
						if (errorsPresent()) {
							break;
						}
						bVPyrlCntrbFndgRfndPybk = (BVPyrlCntrbFndgRfndPybk) aoRfndFndgRqmt.get(j);
						tpfndgrqmt1 = new tpfndgrqmt();
						//1D@P.01
						//tpfndgrqmt1.opt_lck_ctl = JClaretyConstants.ZERO_INTEGER;
						//1A@P.01
						tpfndgrqmt1.opt_lck_ctl = 1;
						tpfndgrqmt1.acct_id = pBEMbrAcct.acct_id;
						tpfndgrqmt1.acct_trans_id = JClaretyConstants.ZERO_INTEGER;
						tpfndgrqmt1.agrmt_id = JClaretyConstants.ZERO_INTEGER;
						tpfndgrqmt1.cntrb_id = bVPyrlCntrbFndgRfndPybk.cntrb_id;
						tpfndgrqmt1.cost_schd_id = bECostSchd.cost_schd_id;
						tpfndgrqmt1.fnc_doc_id = JClaretyConstants.ZERO_INTEGER;
						tpfndgrqmt1.fscl_yr_end_nr = bVPyrlCntrbFndgRfndPybk.fscl_yr_end_nr;
						tpfndgrqmt1.fscl_yr_strt_nr = bVPyrlCntrbFndgRfndPybk.fscl_yr_strt_nr;
						//The contribution amount of the funding requirement is saved in tot_rqd_amt of the buyback funding
						//Rest of the fields are set to zero. Depending on the payment type (pre or post), we are
						//going to move tot_rqd_amt to the pre or post tax bucket
						tpfndgrqmt1.tot_rqd_amt = bVPyrlCntrbFndgRfndPybk.post_tax_amt.add(bVPyrlCntrbFndgRfndPybk.pre_tax_amt
								.add(bVPyrlCntrbFndgRfndPybk.trans_post_tax
										.add(bVPyrlCntrbFndgRfndPybk.trans_pre_tax)))
										.multiply(new BigDecimal(-1.00));
						tpfndgrqmt1.osc_int_post_tax_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.osc_int_pre_tax_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.er_share = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.post_tax_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.pre_tax_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.osc_rqd_int_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.trans_post_tax = JClaretyConstants.ZERO_BIGDECIMAL;
						tpfndgrqmt1.trans_pre_tax = JClaretyConstants.ZERO_BIGDECIMAL;
						aoTPFndgRqmt.add(tpfndgrqmt1);
					}
					//End of changes @RfndChanges
					if (aoTPFndgRqmt != null) {
						dbfndgrqmt.saveTPArray(aoTPFndgRqmt);
						if (errorsPresent()) {
							return null;
						}

						paoTPFndgRqmt = aoTPFndgRqmt;

					}
				}
			}
		}
		addTraceMessage("updateRefundCostScheduleForMbrAcct",FABM_METHODEXIT);
		takeBenchMark("updateRefundCostScheduleForMbrAcct",FABM_METHODEXIT);
		return paoTPFndgRqmt;
	}
	/*********************************************************
	 * Method Name : VerifyPymtInstrNCash()
	 *
	 * Input Parameters :
	 *	P_BVAgrmt : BV_Agrmt 
	 *	P_aoBVUnallocCash : Array of BV_Unalloc_Cash	// can be NIL when Applychecks is called from Prepare Member Account process
	 *	P_Accelerate : Boolean
	 *	P_ApplyAmt : DecimalNullable // NIL for all the cases other that payroll deduction
	 *	P_EffDt : DateTimeData 
	 *	P_BEPymtInstr : BE_Pymt_Instr
	 *
	 * Output Parameters :
	 *    P_BEPymtInstr : BE_Pymt_Instr
	 *	 P_ApplyAmt : DecimalNullable 
	 *
	 * Return Type :
	 *	ErrorStack
	 *
	 * Purpose :
	 *   To verify amount of purchase, payer type & tolerance level
	 *	before applying the payment. Also figure out the payment instruction to be 
	 *	handled in the applychecks method
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE					   jkaur1	   06/25/2008  Migration from FORTE 
	 * NE.02				   jmisra	   09/16/2008  Implementation of new requirement 
	 * 												   for Tab Payment.
	 * C.01	  CCR-77794		   jmisra	   10/30/2009  Modified for the Batch part of the CCR-77794.
	 * P.01		80487		   la		   11/03/2009  Agreement outstanding amt is check first and
	 * 												   assigned to applyamt is actcual apply amt > outstanding amt
	 * P.02		80487		   la			11/04/2009 Modified the fix made for P.01
	 * P.03		80487			la			11/06/2009 BEFncItem is used to caluclate the excess amount
	 * P.04		80487			singhp		11/18/2009 Handled null pointer exception
	 * P.05	 NPRIS-5086		   vgp			02/11/2009 Fix null pointer exception
	 * P.06  NPRIS-5160		   kgupta		11/03/2010 Modified to implement the requirement of $10 tolerance of 
	 *												   overpayment/underpayment OSC payments.
	 * P.07	 NPRIS-5331		   vgp			11/15/2010 When > orig cost is posted for RLVR then LSUM is reduced
	 * 													by the diff, so do not reduce LSUM agin when posting LSUM 	
	 * P.08	 NPRIS-5563		   vgp			12/29/2011	Fix lumpsum overpayment refund issue
	 * P.09	 NPRIS-5485			vgp			02/07/2012	Do not allow any tolerance for EPYR and IPYR portions of MKUP	
	 *************************************************************************************/
	public BVFABMOSCVerifyPymtInstrNCash verifyPymtInstrNCash(BVAgrmt pBVAgrmt, List paoBVUnallocCash,
			boolean pAccelerate, BigDecimal pApplyAmt,Date pEffDt, BEPymtInstr pBEPymtInstr,boolean pActualAlloc) throws ClaretyException {

		startBenchMark("verifyPymtInstrNCash", FABM_METHODENTRY);
		addTraceMessage("verifyPymtInstrNCash",FABM_METHODENTRY);

		BEFncItem beFncItem = new BEFncItem();
		BEPart bepart;
		int pyrId = 0;
		boolean isPostTax = false;
		boolean isPreTax = false;
		BigDecimal acclAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal avblCash = JClaretyConstants.ZERO_BIGDECIMAL; //Unallocated amount from the array of cash 
		BigDecimal applyAmt = JClaretyConstants.ZERO_BIGDECIMAL; //amount to be allocated
		boolean isLump = false, isRlvr = false;
		// At first figure the amount to be applied from the cash .  If P_ApplyAmt is not NIL then skip it
		if (paoBVUnallocCash != null && paoBVUnallocCash.size() > JClaretyConstants.ZERO_INTEGER) {
			for (int row = 0; row < paoBVUnallocCash.size(); row++) {
				BVUnallocCash bvUnallocCash = (BVUnallocCash) paoBVUnallocCash.get(row);
				if (bvUnallocCash.aobefncitem != null && bvUnallocCash.aobefncitem.size() > JClaretyConstants.ZERO_INTEGER) {
					for (int row1 = 0; row1 < bvUnallocCash.aobefncitem.size(); row1++) {
						beFncItem = (BEFncItem) bvUnallocCash.aobefncitem.get(row1);
						avblCash = avblCash.add(beFncItem.item_amt.subtract(beFncItem.from_alloc_amt));
					}
				}
			}
		}
		if (pApplyAmt != null) {
			applyAmt = pApplyAmt;
		}
		else {
			applyAmt = avblCash;
			pApplyAmt = JClaretyConstants.ZERO_BIGDECIMAL;
			pApplyAmt = applyAmt;
		}
		if (pBEPymtInstr != null) {
			if (pBEPymtInstr.accl_in) {
				String[] params = {"Cannot apply cash to the chosen payment . It is already accelerated . "};
				addErrorMsg(1037, params);
				return null;
			}
		}
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// Note: Remember that, if P_Accelerate is TRUE then P_BEPymtInstr is definitely going to be NIL .  This method
		// will try to create PRE and/or POST tax payment instruction ( s )  for acceleration on the basis of the type of
		// cash sent in .
		// At the end all the relevant payment instruction ( s )  are going to be passed back inside P_aoBEPymtInstr
		// Also, the output will always contain only one BEPymtInstr with an exception of acceleration in case of which we might
		// expect two elements ( pre and post )  back
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// At first we are going to find out the payer of the money is there is some cash on hand

		if (paoBVUnallocCash != null && paoBVUnallocCash.size() > JClaretyConstants.ZERO_INTEGER) {
			for (int row = 0; row < paoBVUnallocCash.size(); row++) {
				BVUnallocCash bvUnallocCash = (BVUnallocCash) paoBVUnallocCash.get(row);
				bepart = dbpart.readByFncDoc(bvUnallocCash.befncdoc);
				if (errorsPresent()) {
					return null;
				}
				if (bepart != null) {
					if (bepart instanceof BEPrsn) {
						pyrId = ((BEPrsn) bepart).prsn_id;
					}
					else if (bepart instanceof BEOrg) {
						pyrId = ((BEOrg) bepart).org_id;
					}
					// We are going to assume that user we never choose money from different participants to satisfy
					// a payment instruction .  Now that we have found the payer, let's get out of the for loop
					break;
				}
			}
		}
		if (!pAccelerate) {
			// The following validation should happen only of we are performing actual allocation of money
			if (pActualAlloc) {
				// Validation - 1
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				// Validate Payer .  Payment instruction has the payre id  ( person id or org id )  in it .
				// Every payment instruction can accept cash from that payer only
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				if (pyrId != pBEPymtInstr.pyr_id) {
					//9A@C.01
					List beEmprChngDtl = dbEmprChngDtl.readByPymtInstrIdAndEmprId(pBEPymtInstr.pymt_instr_id, pyrId);
					if(errorsPresent()){
						return null;
					}
					if(beEmprChngDtl == null){
						String[] params = {"Cash has not been received from the correct participant . "};
						addErrorMsg(1037, params);
						return null;
					}
					//3C@C.01
					/*String[] params = {"Cash has not been received from the correct participant . "};
					addErrorMsg(1037, params);
					return null;*/
				}
				// Validation - 2
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				// Now verify the payment type of payment instruction against the cash passed into the method
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				if (paoBVUnallocCash != null && paoBVUnallocCash.size() > JClaretyConstants.ZERO_INTEGER) {
					for (int row = 0; row < paoBVUnallocCash.size(); row++) {
						BVUnallocCash bvUnallocCash = (BVUnallocCash) paoBVUnallocCash.get(row);
						// When an agreement gets payment through payroll deduction, ER stores money under Employer Report at the time of
						// W&C posting .  Later on, OSC uses that money to apply to the agreement .
						// Also in this case we will rely on the alloc type and the alloc amount passed into applychecks
						if (!bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_PYBL)) {
							// Cash can be matched exactly to a payment instruction unless an agreement is accelerated
							// In case of acceleration, we will take any type of cash and satisfy the outstanding amount
							// at one shot
							if (((pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM) &&
									!bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OPST))|| 
									(pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR) && 
											!bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OPRT))
											|| (pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_INST) && 
													!bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OSMP)))) {

								addErrorMsg(1037,new String[]{"Type of cash is not suitable for the agreement . "});
								return null;
							}
						}
					}
				}
				// Validation - 3
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				// Having matched the cash to payment instruction, now figure the amount to be allocated to the
				// payment instruction
				// In case of payroll deductions, employer might send any amount that might or might not match the exact
				// requirement of a payment instruction .  Per business rule, PIONEER is supposed to post as much as is sent
				// without verifying the amount against be_pymt_instr . pymt_amt .
				// But at any point of time, an agreement can accept as much money as is needed to satisfy the agreement .
				// If P_ApplyAmt > amount required to satisfy the agreement, then use only as much as is needed
				// However, for all other cases, payment amount will have to be taken from cash
				////////////////////////////////////////////////////////////////////////////////////////////////////////////-
				// Fact: If a member chooses to pay off an agreement through Lump Sum and Rollover payment then
				// she needs to pay off the Rollover part firt .  If her rollover cash is less that the rollover amount
				// in payment instruction then she has the choice of making up the difference as a part of the lump sum
				// payment
				// We will no verify that P_ApplyAmt is within tolerance level
				// On the basis of above mentioned fact, we also want to figure whether lumpsum amount is to be
				// modified or not
				if (paoBVUnallocCash != null && paoBVUnallocCash.size() > JClaretyConstants.ZERO_INTEGER) {
					if (pBVAgrmt.aobepymtinstr.size() == JClaretyConstants.ONE_INTEGER) {
						BEPymtInstr aobepymtinstr1 = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(0);
						if (aobepymtinstr1.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)
								|| aobepymtinstr1.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR)) {
							//If the cash reported is less than the payment amount  ( the amount needed for the agrmt )
							//by $10 or more, then report error
							BigDecimal subAmount = aobepymtinstr1.pymt_amt.subtract(applyAmt);
							BigDecimal BIG_OSC_TOLERANCE_AMT = new BigDecimal(OSC_TOLERANCE_AMT);
							if ((subAmount).compareTo(BIG_OSC_TOLERANCE_AMT) > JClaretyConstants.ZERO_INTEGER) {
								String[] params = {"Cash is not enough to satisfy the payment . "};
								addErrorMsg(1037, params);
								return null;
							}
						}
					} else {
						BigDecimal diff = JClaretyConstants.ZERO_BIGDECIMAL;
						if (pBVAgrmt.aobepymtinstr.size() > JClaretyConstants.ONE_INTEGER) {
							for (int row = 0; row < pBVAgrmt.aobepymtinstr.size(); row++) {
								BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(row);
								if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)) {
									isLump = true;
								}
								if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR)) {
									isRlvr = true;
									if (bePymtInstr.fnc_item_id != null) {
										BEFncItem tmpBEFncItem = new BEFncItem();
										tmpBEFncItem.fnc_item_id = bePymtInstr.fnc_item_id.intValue();
										tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
										if (errorsPresent()) {
											return null;
										}
										if (tmpBEFncItem != null) {
											diff = bePymtInstr.orig_amt.subtract(tmpBEFncItem.to_alloc_amt);
										}
									}
								}
							}
							if (isLump && isRlvr) {
								//We're handling an agreement which has both lumpsum & rollover pymt instr
								//if we're processing lumpsum, it can be taken for granted that rollover has
								//alreay been processed
								if (pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)) {
									//4A@P.07
									//When rollover is paid if the payment amount is more than the rollover original cost,
									//then the lumpsum payment amount is reduced by the difference between rollover payment 
									//amount and original cost. So do not reduce the diff again at the time of lumpsum payment
									if (diff != null && diff.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {		//1M@P.07
										pBEPymtInstr.pymt_amt = pBEPymtInstr.pymt_amt.add(diff);
									}
									BigDecimal subAmount = pBEPymtInstr.pymt_amt.subtract(applyAmt);
									BigDecimal BIGD_OSC_TOLERANCE_AMT = new BigDecimal(OSC_TOLERANCE_AMT);
									//BigDecimal NEG_BIGD_OSC_TOLERANCE_AMT = new BigDecimal(	-OSC_TOLERANCE_AMT);	//1D@P.08
									if ((subAmount.compareTo(BIGD_OSC_TOLERANCE_AMT)) > JClaretyConstants.ZERO_INTEGER) {	 	//1M@P.08
										//	|| (subAmount.compareTo(NEG_BIGD_OSC_TOLERANCE_AMT)) < JClaretyConstants.ZERO_INTEGER) {		//1D@P.08
										String[] params = {"Cash is not enough to satisfy the payment . "};
										addErrorMsg(1037,params);
										return null;
									}
								}
							}
						}
					}
				}
				// Please make sure that as we update information on P_BEPymtInstr, inside this method, the original
				// payment intruction inside P_BVAgrmt gets updated, too .				
			}
		} else {
			// For acceleration cases, do the following
			// 1 )  For acceleration, only pre or post tax cash can be sent in .  Combenation cannot be allowed .
			// 2 )  For acceleration there is no tolerance .  So P_ApplyAmt should match the acceleration amount to the penny
			// 3 )  Figure out the types of cash passed in .  Depending on that we need to create one acceleration
			//    payment instruction .
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			// Validation - 1 - Combination of pre ans post tax money is not acceptable
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			if (paoBVUnallocCash != null
					&& paoBVUnallocCash.size() > JClaretyConstants.ZERO_INTEGER) {
				for (int row = 0; row < paoBVUnallocCash.size(); row++) {
					BVUnallocCash bvUnallocCash = (BVUnallocCash) paoBVUnallocCash.get(row);
					if (!bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_PYBL)) {
						if ((bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OPST) ||
								bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OSMP))) {
							// post tax money received
							isPostTax = true;
						}
						else if (bvUnallocCash.befncdoc.doc_typ_cd.trim().equals(JClaretyConstants.DOC_TYP_CD_OPRT)) {
							// pre tax money received
							isPreTax = true;
						}
						if (isPostTax && isPreTax) {
							String[] params = {"Combination of PreTax and PostTax money cannot be used for acceleration"};
							addErrorMsg(1037, params);
							break;
						}
					}
					else {
						String[] params = {"Type of cash is not suitable for acceleration"};
						addErrorMsg(1037, params);
						break;
					}
				}
			}
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			// Validation - 2 - Cash should match to the last penny
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			//For makeup agreements, member can acelerate member's part only .  Employer's liability is to be
			//handled by the employer only .  So for makeup type of agreement, make sure that the acceleration
			//amount matches outstanding member contribution
			//For all other types of agreement, make sure that acceleration amount matches total outstanding
			//amount
			BEFncItem befncitem = null;
			if (pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)) {
				if (pBVAgrmt.aobepymtinstr != null	&& pBVAgrmt.aobepymtinstr.size() > JClaretyConstants.ZERO_INTEGER) {
					for (int pymt = 0; pymt < pBVAgrmt.aobepymtinstr.size(); pymt++) {
						BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(pymt);
						if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR)) {
							acclAmt = bePymtInstr.orig_amt.subtract((bePymtInstr.pymt_amt.multiply(
									new BigDecimal(bePymtInstr.nbr_of_pymts_made.intValue()))));
							bePymtInstr.accl_in = true;
							if (bePymtInstr.fnc_item_id != null) {
								befncitem = new BEFncItem();
								befncitem.fnc_item_id = bePymtInstr.fnc_item_id.intValue();
							}
							bePymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt,befncitem, bePymtInstr);
							if (errorsPresent()) {
								return null;
							}
							break;
						}
					}
				}
			}
			else {
				acclAmt = pBVAgrmt.agrmt.tot_cost
				.subtract(pBVAgrmt.agrmt.paid_pre_tax_amt.subtract(pBVAgrmt.agrmt.paid_post_tax_amt));
				if (pBVAgrmt.aobepymtinstr != null	&& pBVAgrmt.aobepymtinstr.size() > JClaretyConstants.ZERO_INTEGER) {
					for (int pymt = 0; pymt < pBVAgrmt.aobepymtinstr.size(); pymt++) {
						BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(pymt);
						if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR)) {
							bePymtInstr.accl_in = true;
							if (bePymtInstr.fnc_item_id != null) {
								befncitem = new BEFncItem();
								befncitem.fnc_item_id = bePymtInstr.fnc_item_id.intValue();
							}
							bePymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt,befncitem, bePymtInstr);
							if (errorsPresent()) {
								return null;
							}
						}
					}
				}
			}
			if (paoBVUnallocCash != null) {
				if ((acclAmt.subtract(applyAmt)).compareTo(
						JClaretyConstants.ZERO_BIGDECIMAL) != JClaretyConstants.ZERO_INTEGER) {
					String[] params = {"Cash is not enough to satisfy acceleration"};
					addErrorMsg(1037, params);
					return null;
				}
			}
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			// If everything is okay then create the acceleration payment instruction
			// ////////////////////////////////////////////////////////////////////////////////////////////////-
			pBEPymtInstr = new BEPymtInstr();
			if (paoBVUnallocCash != null) {
				if (isPostTax) {
					pBEPymtInstr.pymt_typ = JClaretyConstants.PYMT_TYP_ACPT;// post tax cash received
					pBEPymtInstr.pre_tax_in = false;
				}
				else {
					pBEPymtInstr.pymt_typ = JClaretyConstants.PYMT_TYP_ACRT;// pre tax cash received
					pBEPymtInstr.pre_tax_in = true;
				}
			}
			else {
				pBEPymtInstr.pymt_typ = JClaretyConstants.PYMT_TYP_ACPT;// Through PMA, acceleration will be treated as post tax always
				pBEPymtInstr.pre_tax_in = false;
			}
			pBEPymtInstr.pyr_id = pyrId;
			pBEPymtInstr.pyr_typ = JClaretyConstants.OSC_PAYER_TYP_MEMB;
			pBEPymtInstr.orig_amt = acclAmt;
			pBEPymtInstr.pymt_amt = acclAmt;
			pBEPymtInstr.nbr_of_pymts = JClaretyConstants.ONE_INTEGER;
			pBEPymtInstr.nbr_of_pymts_made = new Integer(JClaretyConstants.ZERO_INTEGER);
			pBEPymtInstr.fnc_item_id = null;
			pBEPymtInstr.eff_dt = pEffDt;
			pBEPymtInstr.accl_in = true;
			pBEPymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt, null,pBEPymtInstr);
			if (errorsPresent()) {
				return null;
			}
			if (pBEPymtInstr != null) {
				pBVAgrmt.aobepymtinstr.add(pBEPymtInstr);
				pApplyAmt = pBEPymtInstr.orig_amt;
			}
		}
		////// End of validations
		// If we have come to this point without an error, then yahoo !!!!!
		// Also find out whether we need the cash in full or a part of it is to be used .  However, do not forget the
		// fact that if employers reign .  So whatever they send, has to be applied!!!- that is provided the amount is within
		// the limit that needs to satisfy an agreement .
		// So check only as much if P_ApplyAmt <> NIL !!!
		// BigDecimal agrmtOutstndgAmt = pBVAgrmt.agrmt.tot_cost.subtract(pBVAgrmt.agrmt.paid_post_tax_amt.subtract(pBVAgrmt.agrmt.paid_pre_tax_amt));1D@P.01
		
		/*Delete for P.02
		 * BigDecimal agrmtOutstndgAmt = pBVAgrmt.agrmt.tot_cost.subtract(pBVAgrmt.agrmt.paid_post_tax_amt).subtract(pBVAgrmt.agrmt.paid_pre_tax_amt);//1A@P.01
		//3A@P.01
		if (pApplyAmt.compareTo(agrmtOutstndgAmt) > JClaretyConstants.ZERO_INTEGER) {
			pApplyAmt = agrmtOutstndgAmt;
		}else{*/
		//if (pApplyAmt != null) {1D@P.01
			if (applyAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) != JClaretyConstants.ZERO_INTEGER) {
				pApplyAmt = applyAmt;
			}
			else {
				pApplyAmt = pBEPymtInstr.pymt_amt;
			}
			
			// Out here we are going to check whether the apply amount is going beyond the total worth of the
			// payment instruction .  If so, let's bring the apply amount down .
			// However, if it is found that P_Apply amount is just about greater that the payment amount but less
			// then the total worth, then we will anyway go ahead with the posting because we would want to exhaust
			// the money, even if it would look like pre matured payments are happening
			// Refer  OSC use case under "Installment payment only"
			if (!pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR)) {
				//P.03 starts
				BEFncItem bEFncItem = new BEFncItem();
				//P.05 Starts
				if (pBEPymtInstr.fnc_item_id == null) {
					bEFncItem.fnc_item_id = JClaretyConstants.ZERO_INTEGER;
				} else {
					bEFncItem.fnc_item_id = pBEPymtInstr.fnc_item_id;
				} 
				bEFncItem = dbfncitem.readByFncItem(bEFncItem);
				if (errorsPresent()) {
					return null;
				}//1A@P.04 
				//M@P.09 - Corrected indentation
				if(bEFncItem != null){
					if(bEFncItem.item_amt == null){
						bEFncItem.item_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					if(bEFncItem.to_alloc_amt == null){
						bEFncItem.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					BigDecimal diFFAmt = bEFncItem.item_amt.subtract(bEFncItem.to_alloc_amt);
					//if (pApplyAmt.compareTo(diFFAmt) > JClaretyConstants.ZERO_INTEGER) { // 1D@P.06
					//A@P.09 Starts - No tolerance for EPYR & IPYR
					if (pApplyAmt.compareTo(diFFAmt) > JClaretyConstants.ZERO_INTEGER) {
						if (pBEPymtInstr.pymt_typ.trim().equals(
								JClaretyConstants.PYMT_TYP_EPYR)
								|| pBEPymtInstr.pymt_typ.trim().equals(
										JClaretyConstants.PYMT_TYP_IPYR)
										|| (pApplyAmt.compareTo(diFFAmt.add(new BigDecimal(
												JClaretyConstants.OSC_TOLERANCE_AMT))) > JClaretyConstants.ZERO_INTEGER)) {
							pApplyAmt = diFFAmt;
						}
					}
				}
				//A@P.09 Ends
				//D@P.09 Starts
//				if (pApplyAmt.compareTo(diFFAmt.add(new BigDecimal(JClaretyConstants.OSC_TOLERANCE_AMT))) > JClaretyConstants.ZERO_INTEGER) { // 1A@P.06
//					pApplyAmt = diFFAmt;
//					// This is the amount that will take part in the allocation
//				}
				
//				}// 1A@P.04
				//D@P.09 Ends
				//P.03 ends
				//3D@P.03
				/*if (pApplyAmt.compareTo((pBEPymtInstr.pymt_amt.multiply(new BigDecimal(	pBEPymtInstr.nbr_of_pymts-pBEPymtInstr.nbr_of_pymts_made)))) > JClaretyConstants.ZERO_INTEGER) {
					pApplyAmt = pBEPymtInstr.pymt_amt.multiply(new BigDecimal(pBEPymtInstr.nbr_of_pymts-pBEPymtInstr.nbr_of_pymts_made));
					// This is the amount that will take part in the allocation
				}*/
			}
		//}//1D@P.02
		/*else { 4D@P.01
			if (pApplyAmt.compareTo(agrmtOutstndgAmt) > JClaretyConstants.ZERO_INTEGER) {
				pApplyAmt = agrmtOutstndgAmt;
			}
		}*/
		//Also if we have a combination agreement  ( RLVR and LSUM )  and are handling a rollover payment, accept larger
		//amount, as far as rollover cash does not cross total cost of agreement .  In order to accomodate allocation of
		//larger amount of money, we need to increase worth of thr rollover item so long as it does not cross the total
		//worth of the agreement .
		if (isLump && isRlvr) {
			if (pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR)) {
				BigDecimal totAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BEFncItem tmpBEFncItem = null;
				if (pBEPymtInstr.fnc_item_id != null) {
					tmpBEFncItem = new BEFncItem();
					tmpBEFncItem.fnc_item_id = pBEPymtInstr.fnc_item_id	.intValue();
					tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
					if (errorsPresent()) {
						return null;
					}
					if (tmpBEFncItem != null) {
						totAmt = totAmt.add(tmpBEFncItem.to_alloc_amt);
					}
				}
				if (pApplyAmt != null) {
					totAmt = totAmt.add(pApplyAmt);
				}
				if (totAmt.compareTo(pBVAgrmt.agrmt.tot_cost) <= JClaretyConstants.ZERO_INTEGER && totAmt.compareTo(pBEPymtInstr.pymt_amt) > JClaretyConstants.ZERO_INTEGER) {
					pBEPymtInstr.pymt_amt = totAmt;
					if (tmpBEFncItem != null) {
						tmpBEFncItem.item_amt = pBEPymtInstr.pymt_amt;
						tmpBEFncItem = dbfncitem.save(tmpBEFncItem,pBVAgrmt.agrmt,pBVAgrmt.mbr_acct);
						if (errorsPresent()) {
							return null;
						}
					}
				}
				//NE.02 - begin
				//For combination of payment methods, check if pymt_amt of rollover is > orig_amt of rollover. In that case,
				//subtract the pymt_amt of rollover from the total cost and update the pymt_amt of Lsumpsum with this difference.
				if(pBEPymtInstr.pymt_amt.compareTo(pBEPymtInstr.orig_amt)>JClaretyConstants.ZERO_INTEGER){
					BEFncItem befncitem = null;
					BigDecimal diff = pBVAgrmt.agrmt.tot_cost.subtract(pBEPymtInstr.pymt_amt);
					if (pBVAgrmt.aobepymtinstr != null	&& pBVAgrmt.aobepymtinstr.size() > JClaretyConstants.ZERO_INTEGER) {
						for (int pymt = 0; pymt < pBVAgrmt.aobepymtinstr.size(); pymt++) {
							BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(pymt);
							if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)) {
								bePymtInstr.pymt_amt = diff;
								bePymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt,befncitem, bePymtInstr);
								if (errorsPresent()) {
									return null;
								}
								break;
							}
						}
					}
				}//NE.02 - end
			}
		}
		BVFABMOSCVerifyPymtInstrNCash bvFABMOSCVerifyPymtInstrNCash = new BVFABMOSCVerifyPymtInstrNCash();
		bvFABMOSCVerifyPymtInstrNCash.pApplyAmt = pApplyAmt;
		bvFABMOSCVerifyPymtInstrNCash.pBEPymtInstr = pBEPymtInstr;
		addTraceMessage("verifyPymtInstrNCash", FABM_METHODEXIT);
		takeBenchMark("verifyPymtInstrNCash", FABM_METHODEXIT);
		return bvFABMOSCVerifyPymtInstrNCash;
	}
	/**********************************************************************************************
	 * Purpose :
	 *       Apply checks paid by member against any given time.
	 *	This method apply the checks sent it against the payment instruction.
	 *	If the Allocation is set to false then the G/L is not performed 
	 *	NOTE: Expected always one payment intruction to be allocated at a time, the caller of this 
	 *	method if has more than one payment instruction to allocate, must send one payment instruction 
	 *	at a time what this means is  the aoPymtInstr in BV_Agrmt can not have more than one element	
	 * @param 	BEMbr
	 * @param 	java.util.Date
	 * @param 	BVAgrmtSmry
	 * @param 	boolean
	 * @param 	boolean
	 * @param 	boolean
	 * @param 	java.math.BigDecimal
	 * @param 	java.math.BigDecimal
	 * @param 	BEPln
	 * @param 	java.util.Vector
	 * @param 	boolean
	 * @param 	java.util.Date
	 * @param 	java.util.Date
	 * @return	BVFABMOSCapplyChecks
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE					   jkaur1	   06/24/2008  Migration from FORTE 
	 * P.01   80807            vgp         10/27/2009  Out of state service purchase posted 
	 * 												   on a prior date displayed as pending 
	 * P.02   77795			   singhp	   11/23/2009  CCR- NPRIS045 - Fix for A/R creation while completing agreement.
	 * P.03   77795			   singhp	   12/29/2009  CCR- NPRIS045 - Fix for posting the amount collected (as opposed the calculated amount). The LINT should be based upon the collected amount.
	 * P.04   77795			   singhp	   01/29/2010  Fix for saving agrmt ouststanding amt in BVAgrmt.
	 * P.05   77795			   singhp	   02/08/2010  Fix for stopping AR Creation if within Tolerance amt.	
	 * P.06   77795			   singhp	   02/10/2010  Fix for consider MPYR Outstanding Amt and not the whole amt.
	 * P.07   NPRIS-1194	   gpannu	   02/16/2010  un-commented code which was commented for CCR 45 and make it call only for non-mkup cases
	 * P.08  NPRIS-5160		   kgupta		11/03/2010 Modified to implement the requirement of $10 tolerance of 
	 *												   overpayment/underpayment OSC payments.
	 * P.09	NPRIS-5161			vgp			04/12/2011 Post paid amt to member when the diff with agrmt amt is within tolerance
	 * P.10	NPRIS-5449			vgp			06/06/2011 Fix to create EMAR for EPYR and IPYR when completing agreement.
	 * P.11	NPRIS-5533			vgp			01/25/2012 Makeups should post only at the completion of employee portion of the cost
	 * P.12	NPRIS-5485			vgp			01/25/2012 Do not allow any tolerance for EPYR and IPYR portions of MKUP
	 * P.13	NPRIS-6265		   VenkataM		10/22/2015 Modified to not complete the agreement for Rollover if unallocated amount is available 
	 *************************************************************************************/
	public BVFABMOSCapplyChecks applyChecks(BEPln pBEPln,
			Date pEffDt, BVAgrmt pBVAgrmt,
			boolean pActualAlloc, Date pPstngDt,
			BigDecimal pApplyAmt, BEPymtInstr pBEPymtInstr,
			boolean pAccelerate, List paoBVUnallocCash,
			boolean pPMA, boolean pHideTransIn,
			boolean pFullyAllocatedIN,boolean agrmtCompletionIN)//1M@P.02
	throws ClaretyException {
		startBenchMark("applyChecks", FABM_METHODENTRY);
		addTraceMessage("applyChecks",FABM_METHODENTRY);

		BEFncItem beFncItem = new BEFncItem();

		List aoToFncItems = new ArrayList();
		List aoFromFncItems = new ArrayList();
		boolean isFirstPymt = false;
		BigDecimal agrmtOutstndgAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BEDocStatHist bedocstathist = new BEDocStatHist();
		boolean createRcvbl = true;//1A@P.05
		/*
		 Perform verification on payment intruction && cash
		 */
		BVFABMOSCVerifyPymtInstrNCash bvFABMOSCVerifyPymtInstrNCash = this.verifyPymtInstrNCash(pBVAgrmt,paoBVUnallocCash, pAccelerate, pApplyAmt,pEffDt, pBEPymtInstr, pActualAlloc);
		if (errorsPresent()) {
			return null;
		}
		pApplyAmt = bvFABMOSCVerifyPymtInstrNCash.pApplyAmt;
		pBEPymtInstr = bvFABMOSCVerifyPymtInstrNCash.pBEPymtInstr;
		BigDecimal pUsedAmt = pApplyAmt;	
		// At this point, cash type, amount to be applied and payment instruction are verified and validated
		/*
		Build the To items
		 */
		BEFncItem tmpBEFncItem = null;
		
		if (pBEPymtInstr != null) {
			if (pBEPymtInstr.fnc_item_id != null && 
					pBEPymtInstr.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
				tmpBEFncItem = new BEFncItem();
				tmpBEFncItem.fnc_item_id = pBEPymtInstr.fnc_item_id.intValue();
				tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
				if (errorsPresent()) {
					return null;
				}
				// 5A@P.08
				// 5D@P.09
//				if(tmpBEFncItem.to_alloc_amt.add(pApplyAmt).compareTo(tmpBEFncItem.item_amt) > 0
//						&& tmpBEFncItem.to_alloc_amt.add(pApplyAmt).subtract(tmpBEFncItem.item_amt)
//						.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= 0){
//					tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt.add(pApplyAmt);
//				}
				//A@P.09 begin
				//D@P.12 Starts
//				if ((tmpBEFncItem.to_alloc_amt.add(pApplyAmt).compareTo(
//						tmpBEFncItem.item_amt) > 0 && tmpBEFncItem.to_alloc_amt
//						.add(pApplyAmt).subtract(tmpBEFncItem.item_amt)
//						.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)
//						||
//						// if paid amount is less
//						(tmpBEFncItem.to_alloc_amt.add(pApplyAmt).compareTo(
//								tmpBEFncItem.item_amt) < 0 && tmpBEFncItem.item_amt
//								.subtract(
//										tmpBEFncItem.to_alloc_amt
//												.add(pApplyAmt)).compareTo(
//										new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)) {
//					tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt
//							.add(pApplyAmt);
//				}
				//A@P.09 end
				//D@P.12 end
				//A@P.12 Start
				if (pBEPymtInstr.pymt_typ.trim().equals(
						JClaretyConstants.PYMT_TYP_IPYR)
						|| pBEPymtInstr.pymt_typ.trim().equals(
								JClaretyConstants.PYMT_TYP_EPYR)) {
					if (pApplyAmt.compareTo(tmpBEFncItem.item_amt
							.subtract(tmpBEFncItem.to_alloc_amt)) > JClaretyConstants.ZERO_INTEGER) {
						pApplyAmt = tmpBEFncItem.item_amt
								.subtract(tmpBEFncItem.to_alloc_amt);
						pUsedAmt = pApplyAmt;
					}
				// when install amt <= toleranec need some additonal check
				} else if (pBEPymtInstr.pymt_amt.compareTo(new BigDecimal(
						OSC_TOLERANCE_AMT)) <= JClaretyConstants.ZERO_INTEGER) {
					//if total paid is less than total owed then only allow one installment tolerance
					if ((tmpBEFncItem.item_amt.subtract(
							tmpBEFncItem.to_alloc_amt.add(pApplyAmt)).abs()
							.compareTo(pBEPymtInstr.pymt_amt) <= JClaretyConstants.ZERO_INTEGER)
							//if total paid is more than total owed then only allow one max tolerance
							|| (tmpBEFncItem.to_alloc_amt.add(pApplyAmt)
									.compareTo(tmpBEFncItem.item_amt) > 0 && tmpBEFncItem.to_alloc_amt
									.add(pApplyAmt).subtract(
											tmpBEFncItem.item_amt).compareTo(
											new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)) {
						tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt
								.add(pApplyAmt);
					}

				} else if (tmpBEFncItem.item_amt.subtract(
						tmpBEFncItem.to_alloc_amt.add(pApplyAmt)).abs()
						.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= JClaretyConstants.ZERO_INTEGER) {
					tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt
							.add(pApplyAmt);
				}
				// A@P.12 End
			}
			else {
				tmpBEFncItem = new BEFncItem();
				tmpBEFncItem.from_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
				tmpBEFncItem.item_amt = pBEPymtInstr.pymt_amt
				.multiply(new BigDecimal(pBEPymtInstr.nbr_of_pymts));
				tmpBEFncItem.item_typ_cd = pBEPymtInstr.pymt_typ.trim();
				tmpBEFncItem.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
				// 5A@P.08
				// 5D@P.09
//				if(pApplyAmt.compareTo(pBEPymtInstr.pymt_amt.multiply(new BigDecimal(pBEPymtInstr.nbr_of_pymts))) > 0
//						&& pApplyAmt.subtract(pBEPymtInstr.pymt_amt.multiply(new BigDecimal(pBEPymtInstr.nbr_of_pymts)))
//						.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= 0){
//					tmpBEFncItem.item_amt = pApplyAmt;
//				}
				//D@P.12 Start
//				if ((pApplyAmt.compareTo(pBEPymtInstr.pymt_amt
//						.multiply(new BigDecimal(pBEPymtInstr.nbr_of_pymts))) > 0 && pApplyAmt
//						.subtract(
//								pBEPymtInstr.pymt_amt.multiply(new BigDecimal(
//										pBEPymtInstr.nbr_of_pymts))).compareTo(
//								new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)
//						|| (pApplyAmt.compareTo(pBEPymtInstr.pymt_amt
//								.multiply(new BigDecimal(
//										pBEPymtInstr.nbr_of_pymts))) < 0 && pBEPymtInstr.pymt_amt
//								.multiply(
//										new BigDecimal(
//												pBEPymtInstr.nbr_of_pymts))
//								.subtract(pApplyAmt).compareTo(
//										new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)) {
//					tmpBEFncItem.item_amt = pApplyAmt;
//				}
				//D@P.12 End
				//A@P.12 Start
				if (pBEPymtInstr.pymt_typ.trim().equals(
						JClaretyConstants.PYMT_TYP_IPYR)
						|| pBEPymtInstr.pymt_typ.trim().equals(
								JClaretyConstants.PYMT_TYP_EPYR)) {
					if (pApplyAmt
							.compareTo(pBEPymtInstr.pymt_amt
									.multiply(new BigDecimal(
											pBEPymtInstr.nbr_of_pymts))) > JClaretyConstants.ZERO_INTEGER) {
						pApplyAmt = pBEPymtInstr.pymt_amt
								.multiply(new BigDecimal(
										pBEPymtInstr.nbr_of_pymts));
						pUsedAmt = pApplyAmt;
					}
				// when install amt <= toleranec need some additonal check
				} else if (pBEPymtInstr.pymt_amt.compareTo(new BigDecimal(
						OSC_TOLERANCE_AMT)) <= JClaretyConstants.ZERO_INTEGER) {
					//if total paid is less than total owed then only allow one installment tolerance
					if ((tmpBEFncItem.item_amt.subtract(
							tmpBEFncItem.to_alloc_amt.add(pApplyAmt)).abs()
							.compareTo(pBEPymtInstr.pymt_amt) <= JClaretyConstants.ZERO_INTEGER)
							//if total paid is more than total owed then only allow one max tolerance
							|| (tmpBEFncItem.to_alloc_amt.add(pApplyAmt)
									.compareTo(tmpBEFncItem.item_amt) > 0 && tmpBEFncItem.to_alloc_amt
									.add(pApplyAmt).subtract(
											tmpBEFncItem.item_amt).compareTo(
											new BigDecimal(OSC_TOLERANCE_AMT)) <= 0)) {
						tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt
								.add(pApplyAmt);
					}
				} else if (pApplyAmt.subtract(
						pBEPymtInstr.pymt_amt.multiply(new BigDecimal(
								pBEPymtInstr.nbr_of_pymts))).abs().compareTo(
						new BigDecimal(OSC_TOLERANCE_AMT)) <= 0) {
					tmpBEFncItem.item_amt = pApplyAmt;
				}
				// A@P.12 End
				
				tmpBEFncItem = fabmgltrans.saveFncItem(pBVAgrmt.agrmt, pBVAgrmt.mbr_acct,tmpBEFncItem, pEffDt, pPstngDt);
				if (errorsPresent()) {
					return null;
				}
				if (tmpBEFncItem != null) {
					//Attach the newly created financial item to the corresponding payment instruction
					pBEPymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt,tmpBEFncItem, pBEPymtInstr);
					if (errorsPresent()) {
						return null;
					}
				}
			}	//1A@P.12 Also modified the format below
			// A@P.10 Starts
			// When MPYR is fully paid system must create an EMAR for any
			// remaining EPYR & IPYR amounts and
			// complete the agreement. The follwoing fix will create EMAR as
			// expected when no EPYR and/or IPYR
			// installments are paid yet. Normally this will happen when the
			// agreement is paid in one istallment.
			if (agrmtCompletionIN
					&& pBVAgrmt.aobepymtinstr != null
					&& pBEPymtInstr.pymt_typ.trim().equals(
							JClaretyConstants.PYMT_TYP_MPYR)) {
				for (int pymtIndx = 0; pymtIndx < pBVAgrmt.aobepymtinstr.size(); pymtIndx++) {
					BEPymtInstr lclBEPymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr
							.get(pymtIndx);
					if (lclBEPymtInstr != null
							&& (lclBEPymtInstr.fnc_item_id == null || lclBEPymtInstr.fnc_item_id == JClaretyConstants.ZERO_INTEGER)
							&& (lclBEPymtInstr.pymt_typ.trim().equals(
									JClaretyConstants.PYMT_TYP_EPYR) || lclBEPymtInstr.pymt_typ
									.trim().equals(
											JClaretyConstants.PYMT_TYP_IPYR))) {
						BEFncItem lclBEFncItem = new BEFncItem();
						lclBEFncItem.from_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						lclBEFncItem.item_amt = lclBEPymtInstr.pymt_amt
								.multiply(new BigDecimal(
										lclBEPymtInstr.nbr_of_pymts));
						lclBEFncItem.item_typ_cd = lclBEPymtInstr.pymt_typ
								.trim();
						lclBEFncItem.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						lclBEFncItem = fabmgltrans.saveFncItem(pBVAgrmt.agrmt,
								pBVAgrmt.mbr_acct, lclBEFncItem, pEffDt,
								pPstngDt);
						if (errorsPresent()) {
							return null;
						}
						if (tmpBEFncItem != null) {
							lclBEPymtInstr = dbPymtInstr.save(pBVAgrmt.agrmt,
									lclBEFncItem, lclBEPymtInstr);
							if (errorsPresent()) {
								return null;
							}
						}

					}

				}
			}
			//A@P.10 ends
//			}	//D@P.12
		}
		if (tmpBEFncItem != null) {
			aoToFncItems = new ArrayList();
			aoToFncItems.add(tmpBEFncItem);
		}
		/*
		 Build the From items
		 */
		BigDecimal totUnallocAmount = JClaretyConstants.ZERO_BIGDECIMAL;
		if (pActualAlloc == true) {
			int counter = 0;
			if (paoBVUnallocCash != null) {
				for (int i = 0; i < paoBVUnallocCash.size(); i++) {
					BVUnallocCash bvUnallocCash = (BVUnallocCash) paoBVUnallocCash.get(i);
					for (int j = 0; j < bvUnallocCash.aobefncitem.size(); j++) {
						beFncItem = (BEFncItem) bvUnallocCash.aobefncitem.get(j);
						aoFromFncItems.add(counter,	beFncItem);
						counter = counter + 1;
					}
				}
			}
		}
		/*
		Perform allocation
		 */
		//Added ErrorPresent
		if (pActualAlloc) {
			BVFABMGLTransallocateItems bvFABMGLTransallocateItems = 
				fabmgltrans.allocateItems(aoFromFncItems,aoToFncItems, pApplyAmt, pEffDt,pPstngDt);
			if (errorsPresent()) {
				return null;
			}
			aoFromFncItems = bvFABMGLTransallocateItems.aoFromFncItems;
			aoToFncItems = bvFABMGLTransallocateItems.aoToFncItems;
		}
		else {
			if(tmpBEFncItem != null){
				tmpBEFncItem.to_alloc_amt = tmpBEFncItem.to_alloc_amt.add(pApplyAmt);
				tmpBEFncItem = fabmgltrans.saveFncItem(pBVAgrmt.agrmt, pBVAgrmt.mbr_acct,tmpBEFncItem, pEffDt, pPstngDt);
				if (errorsPresent()) {
					return null;
				}
			}
		}
		FABMWageAndCntrb fabmWageAndCntrb = (FABMWageAndCntrb)getFABMInstance(FABMWageAndCntrb.class);
		/*
		Set The cash status to allocated if  fully allocated
		 */
		if (pActualAlloc) {
			bedocstathist.doc_stat_hist_opt_lck_ctl = JClaretyConstants.ZERO_INTEGER;
			bedocstathist.doc_stat_hist_id = JClaretyConstants.ZERO_INTEGER;
			bedocstathist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_A;
			bedocstathist.doc_stat_dt = getTimeMgr().getCurrentDateOnly();
			//Set the cash receipt status if fully allocated
			fabmWageAndCntrb.calcAndSetExcCashStatus(bedocstathist, paoBVUnallocCash);
			if (errorsPresent()) {
				return null;
			}
		}
		/*
		 Update the agreement attributes to reflect the payment
		 */
		// update information on be_pymt_instr
		pBEPymtInstr.last_pymt_dt = pEffDt;
		if (pBEPymtInstr.nbr_of_pymts_made == null) {
			pBEPymtInstr.nbr_of_pymts_made = new Integer(JClaretyConstants.ONE_INTEGER);
		}else {
			pBEPymtInstr.nbr_of_pymts_made = new Integer(pBEPymtInstr.nbr_of_pymts_made.intValue()	+ JClaretyConstants.ONE_INTEGER);
		}
		if (pBVAgrmt.agrmt.paid_pre_tax_amt	.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER
				&& pBVAgrmt.agrmt.paid_post_tax_amt	.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER) {
			isFirstPymt = true;
		}
		if (pBEPymtInstr.pre_tax_in) {//doubt
			pBVAgrmt.agrmt.paid_pre_tax_amt	= pBVAgrmt.agrmt.paid_pre_tax_amt.add(pApplyAmt);
		}
		else {
			pBVAgrmt.agrmt.paid_post_tax_amt = pBVAgrmt.agrmt.paid_post_tax_amt.add(pApplyAmt);
		}
		/*
		 if (  first payment ){ change the status of the contribution to AGMT
		 */
		if (isFirstPymt) {
			//Array of BVPyrlDtl
			List aoBVPyrlDtl;
			//Array of TP_Srv_Stat_Hist
			List aoTPSrvStatHist = new ArrayList();
			aoBVPyrlDtl = dbfndgrqmt.readBVPyrlDtlByAgrmt(pBVAgrmt.agrmt,pBVAgrmt.mbr_acct.acct_id);
			if (errorsPresent()) {
				return null;
			}
			if (aoBVPyrlDtl != null) {
				for (int i = 0; i < aoBVPyrlDtl.size(); i++) {
					BVPyrlDtl bvPyrlDtl = (BVPyrlDtl) aoBVPyrlDtl.get(i);
					TpSrvStatHist tpSrvStatHist = new TpSrvStatHist();
					tpSrvStatHist.acct_id = pBVAgrmt.mbr_acct.acct_id;
					tpSrvStatHist.cntrb_id = bvPyrlDtl.becntrb.cntrb_id;
					tpSrvStatHist.cntrb_stat_cd = JClaretyConstants.CNTRB_STAT_CD_AGMT;
					//5A@P.01
					if (pEffDt != null) {
						tpSrvStatHist.stat_chg_dt = pEffDt;
					} else {
						tpSrvStatHist.stat_chg_dt = pPstngDt;
					}
					//1D@P.01
					//tpSrvStatHist.stat_chg_dt = pPstngDt;
					aoTPSrvStatHist.add(tpSrvStatHist);
				}
				dbcntrb.updateCntrbStatArray(aoTPSrvStatHist);
				if (errorsPresent()) {
					return null;
				}
			}
			if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_A)) {
				pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_I;
			}
		}
		/*
		Final touch on the agreement
		 */
		// ////////////////////////////////////////////////////////////////////////////////////////////////////
		// We want to figure out whether the agreement is anywhere near completion .  If it is <= $10 away from
		// getting completed then it is upto the ret sys to make up that money .
		// On the other hand if the cash is from member and is in excess by <= $10 then ret sys does not want to bear
		// the hassle of returning it .  So apply to cash to ret sys .  ???  ( this part is still a problem .  donno how to identify cash .  talk to Meskel )
		// So we need either a receivable or a payable for the ret sys and perform some allocation
		// //////////////////////////////////////////////////////////////////////////////////////////////////-
		// First of all check agreement outstanding amount
		agrmtOutstndgAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		agrmtOutstndgAmt = pBVAgrmt.agrmt.tot_cost
		.subtract(pBVAgrmt.agrmt.paid_pre_tax_amt).subtract(pBVAgrmt.agrmt.paid_post_tax_amt);
		//@P.13 STARTS
		if (!pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(
				JClaretyConstants.SRV_TYP_CD_MKUP)
				&& pBEPymtInstr.pymt_typ.trim().equals(
						JClaretyConstants.PYMT_TYP_RLVR)) {
			for (int row = 0; row < pBVAgrmt.aobepymtinstr.size(); row++) {
				BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr
						.get(row);
				if (bePymtInstr.pymt_typ.trim().equals(
						JClaretyConstants.PYMT_TYP_LSUM)
						&& bePymtInstr.pymt_amt
								.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
					pFullyAllocatedIN = false;
					break;
				}

			}
		}
		//@P.13 ENDS
		// If outstanding amount is within tolerance, agreement is as good as completed .
		// If outstanding amount is within tolerance but > $0, then arrange for ret sys payment .
		// added an indicator P_FullyAllocated_IN to avoid creating NPERS receivables until the last item in the payment instruction was reached .
		if (agrmtOutstndgAmt.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= JClaretyConstants.ZERO_INTEGER && pFullyAllocatedIN) {
			//D7@P.09 Since we posting what is paid no need to allocate the difference
//			if (agrmtOutstndgAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
//				this.allocateTolerance(pBEPln, pBVAgrmt,paoBVUnallocCash, pEffDt,
//						pPstngDt,agrmtOutstndgAmt, false,pActualAlloc);
//				if (errorsPresent()) {
//					return null;
//				}
//			}
			//To allow only NPERS receivables .
			if (agrmtOutstndgAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
				//2A@P.07
				//5D@P.09
//				if(!pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)){
//					pBVAgrmt.agrmt.paid_pre_tax_amt = pBVAgrmt.agrmt.paid_pre_tax_amt.add(agrmtOutstndgAmt);//1D@P.03
//				}else{
//					pBVAgrmt.agrmt.paid_pre_tax_amt = pBVAgrmt.agrmt.paid_pre_tax_amt;//1A@P.03
//				}
				//pBVAgrmt.agrmtOustStandingAmt = agrmtOutstndgAmt;//1A@P.04//1D@P.06
				BEFncDoc tempBeFncDoc = new BEFncDoc();//16A@P.06
				List tempAoFncItem = new ArrayList();
				BEFncItem tempbeFncItem = new BEFncItem();
				tempBeFncDoc.fnc_doc_id = pBVAgrmt.agrmt.fnc_doc_id;
				tempAoFncItem = dbfncitem.readAllByFncDoc(tempBeFncDoc);
				if(errorsPresent()){
					return null;
				}
				if(tempAoFncItem != null){
					for(int j=0;j<tempAoFncItem.size();j++){
						tempbeFncItem = (BEFncItem)tempAoFncItem.get(j);
						if(tempbeFncItem.item_typ_cd.trim().equalsIgnoreCase(JClaretyConstants.PYMT_TYP_MPYR)){
							pBVAgrmt.agrmtOustStandingAmt = tempbeFncItem.item_amt.subtract(tempbeFncItem.to_alloc_amt);
						}
					}
				}
				createRcvbl = false;//1A@P.05
				agrmtOutstndgAmt = JClaretyConstants.ZERO_BIGDECIMAL;
			}
		}
		/*
		Save the agreement
		 */		
		boolean pFlgAgrmtPaid = false;
		//D@P.11 Starts
//		// To allow both NPERS receivables and payables to complete the agreement .
//		if ((agrmtOutstndgAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER && pFullyAllocatedIN != false)
//				|| (agrmtOutstndgAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) < JClaretyConstants.ZERO_INTEGER && pFullyAllocatedIN != false)) {
//			pFlgAgrmtPaid = true;
//			pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_C;
//			// If the agreement is paid is full, go ahead and check whether there is any extra money within tolerance
//			// If found then allocate the money to ret sys
//			// D4@P.09 Since we posting what is paid no need to allocate the difference
////			this.allocateTolerance(pBEPln, pBVAgrmt,paoBVUnallocCash, pEffDt, pPstngDt,null, true, pActualAlloc);
////			if (errorsPresent()) {
////				return null;
////			}
//		}
//		else {
//			pFlgAgrmtPaid = false;
//		}
//		if(agrmtCompletionIN){
//			pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_C;
//		}
		//D@P.11 Ends
		//A@P.11 Starts
		if (pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)) {
			if (pBEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR) && (agrmtCompletionIN)) {
				pFlgAgrmtPaid = true;
				pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_C;
			}
		} else if (agrmtOutstndgAmt
				.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) <= JClaretyConstants.ZERO_INTEGER
				&& pFullyAllocatedIN != false) {
			pFlgAgrmtPaid = true;
			pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_C;
		}
		//A@P.11 Ends
		
		if(createRcvbl){//5A@P.05
			pBVAgrmt.createRcvbl = true;
		}else{
			pBVAgrmt.createRcvbl = false;
		}
		BVFabmOscSaveAgrmt pBVFabmOscSaveAgrmt = null;
		BEOSCRqst beoscrqst = null;
		beoscrqst = dboscrqst.readbyOSCAgrmt(pBVAgrmt);
		if (errorsPresent()) {
			return null;
		}
		pBVFabmOscSaveAgrmt = this.saveAgrmt(pBEPln,pEffDt, beoscrqst, pPMA, pPstngDt,pBVAgrmt,
				true, null, pPMA, pHideTransIn);
		if (errorsPresent()) {
			return null;
		}
		BVFABMOSCapplyChecks bvFABMOSCapplyChecks = new BVFABMOSCapplyChecks();
		bvFABMOSCapplyChecks.paoBVUnallocCash = paoBVUnallocCash;
		bvFABMOSCapplyChecks.pFlgAgrmtPaid = pFlgAgrmtPaid;
		bvFABMOSCapplyChecks.pPstdSrvCrdtAmt = pBVFabmOscSaveAgrmt.pPstdSrvCrdtAmt;
		bvFABMOSCapplyChecks.pUsedAmt = pUsedAmt;
		addTraceMessage("applyChecks", FABM_METHODEXIT);
		takeBenchMark("applyChecks", FABM_METHODEXIT);
		return bvFABMOSCapplyChecks;
	}
	/*********************************************************
	 * Method Name : AllocateTolerance()
	 *
	 * Input Parameters :
	 *		P_BEPln				:	BE_Pln
	 *		P_BVAgrmt			:	BV_Agrmt
	 *		P_aoBVUnallocCash	:	Array of BV_Unalloc_Cash
	 *		P_EffDt				:	DateTimeData
	 *		P_PstngDt			:	DateTimeData
	 *		P_ToleranceAmt		:	DecimalData
	 *		P_FromAgrmt		:	Boolean
	 *
	 * Output Parameters :
	 *	None
	 *
	 * Return Type :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	Allocate money from the ret sys for the agreement* Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE					   jkaur1	   06/24/2008  Migration from FORTE
	 *************************************************************************************/
	public void allocateTolerance(BEPln pBEPln,BVAgrmt pBVAgrmt, List paoBVUnallocCash,Date pEffDt, Date pPstngDt,
			BigDecimal pToleranceAmt, boolean pFromAgrmt,boolean pActualAlloc) throws ClaretyException {
 
		startBenchMark("allocateTolerance", FABM_METHODENTRY);
		addTraceMessage("allocateTolerance",FABM_METHODENTRY);
		BEFncItem beFncItem = new BEFncItem();
		BEFncItem tmpBEFncItem;
		//Array of BEFncItem
		List aoToFncItems = new ArrayList();
		//Array of BEFncItem
		List aoFromFncItems = new ArrayList();
		//FABMWageAndCntrb fabmWageAndCntrb = (FABMWageAndCntrb) getFABMInstance(FABMWageAndCntrb.class);
		if (!pFromAgrmt) {
			// Agreement needs money, get from the retirement system

			// Create a new fnc item for agreement to receive the money
			tmpBEFncItem = new BEFncItem();
			tmpBEFncItem.from_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
			tmpBEFncItem.item_amt = pToleranceAmt;
			tmpBEFncItem.item_typ_cd = JClaretyConstants.ITEM_TYP_CD_OUOP;
			tmpBEFncItem.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
			beFncItem = fabmgltrans.saveFncItem(pBVAgrmt.agrmt, pBVAgrmt.mbr_acct,tmpBEFncItem, pEffDt, pPstngDt);
			if (errorsPresent()) {
				return;
			}
			// Ret sys makes up the AgrmtOutstndgAmt
			// Create receivable for ret sys
			//Create Doc Stat Hist
			BEDocStatHist bedocstathist = new BEDocStatHist();
			BVRetSys bvretsys;
			BEAcctRcvbl beacctrcvbl = new BEAcctRcvbl();
			BigDecimal rcvblAmt = pToleranceAmt;
			bedocstathist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_E;
			bedocstathist.doc_stat_dt = pEffDt;
			//Build Acct Rcvbl
			beacctrcvbl.doc_dt = pEffDt;
			beacctrcvbl.doc_fsc_yr = dbfscyr.calculateFiscalYearNr(pBEPln, pEffDt);
			if (errorsPresent()) {
				return;
			}
			beacctrcvbl.doc_typ_cd = JClaretyConstants.DOC_TYP_CD_AR;
			bvretsys = dbRetSys.readByPln(pEffDt, pBEPln);
			if (errorsPresent()) {
				return;
			}
//			create new receivable
			beacctrcvbl = fabmwageandcntrb.saveAcctRcvbl(beacctrcvbl, pBEPln, null,	bedocstathist, null, rcvblAmt,
					pPstngDt, pEffDt, (BERetSys) bvretsys,true, JClaretyConstants.DOC_TYP_CD_AR);
			if (errorsPresent()) {
				return;
			}// End of receivable creation
			if (beacctrcvbl != null) {
				// Read all fnc items attached to the receivable
				aoFromFncItems = dbfncitem.readAllByFncDoc(beacctrcvbl);
				if (errorsPresent()) {
					return;
				}
			}
			if (tmpBEFncItem != null) {
				if (pActualAlloc) {
					aoToFncItems = new ArrayList();
					aoToFncItems.add(0, tmpBEFncItem);
					BVFABMGLTransallocateItems bvFABMGLTransallocateItems = fabmgltrans.allocateItems(aoFromFncItems,aoToFncItems,pToleranceAmt, pEffDt,pPstngDt);
					if (errorsPresent()) {
						return;
					}
					aoFromFncItems = bvFABMGLTransallocateItems.aoFromFncItems;
					aoToFncItems = bvFABMGLTransallocateItems.aoToFncItems;
				} else {
					tmpBEFncItem.to_alloc_amt = tmpBEFncItem.to_alloc_amt.add(pToleranceAmt);
					beFncItem = fabmgltrans.saveFncItem(pBVAgrmt.agrmt,	pBVAgrmt.mbr_acct,tmpBEFncItem, pEffDt, pPstngDt);
					if (errorsPresent()) {
						return;
					}
				}
			}
		}

		addTraceMessage("allocateTolerance", FABM_METHODEXIT);
		takeBenchMark("allocateTolerance", FABM_METHODEXIT);
	}
	/*********************************************************
	 * Method Name : DecideIfEmprCntrbIsOverridden()
	 *
	 * Input Parameters	:
	 *				P_BEPln:BE_Pln
	 *				P_BEMbrAcct:BE_Mbr_Acct
	 *				P_BEPyrl:BE_Pyrl
	 *				P_BEEmpr:BE_Empr
	 *				P_EmprCntrb:DecimalData 
	 * Output Parameters	:
	 *	 			P_IsOverridden:boolean
	 *				P_Compare:integer
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method decides if the employer Contribution is overriden
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 nyadav		08/21/2008  Migration from forte.
	 *************************************************************************************/
	public BVFABMOSCDecideIfEmprCntrbIsOverridden decideIfEmprCntrbIsOverridden(BEPln pBEPln, BEMbrAcct pBEMbrAcct, BEPyrl pBEPyrl,
			BEEmpr pBEEmpr, BigDecimal pEmprCntrb) throws ClaretyException {
		addTraceMessage("decideIfEmprCntrbIsOverridden", FABM_METHODENTRY);
		startBenchMark("decideIfEmprCntrbIsOverridden", FABM_METHODENTRY);
		BVFABMOSCDecideIfEmprCntrbIsOverridden bvFABMOSCDecideIfEmprCntrbIsOverridden = new BVFABMOSCDecideIfEmprCntrbIsOverridden();
		boolean pIsOverridden = false;
		int pCompare = JClaretyConstants.ZERO_INTEGER;
		if(pBEPln.pln_id == JClaretyConstants.PLN_ID_JUDGES || pBEPln.pln_id == JClaretyConstants.PLN_ID_JDGS_7){
			pIsOverridden = true;
		}else{
			BVFabmMbrAcctMaintCalcContrb bvFabmMbrAcctMaintCalcContrb  = fabmmbracctmaint.
			calcContribution(pBEMbrAcct, pBEPyrl, pBEEmpr, null);
			if(errorsPresent()){
				return null;
			}
			if(bvFabmMbrAcctMaintCalcContrb.ERCntrbAmt.compareTo(pEmprCntrb)==JClaretyConstants.ZERO_INTEGER){
				pCompare = JClaretyConstants.ZERO_INTEGER; 
			}else if(bvFabmMbrAcctMaintCalcContrb.ERCntrbAmt.compareTo(pEmprCntrb)>JClaretyConstants.ZERO_INTEGER){
				pCompare = -1; 
				pIsOverridden = true;
			}else{
				pCompare = 1; 
				pIsOverridden = true;
			}
		}
		bvFABMOSCDecideIfEmprCntrbIsOverridden.pCompare = pCompare;
		bvFABMOSCDecideIfEmprCntrbIsOverridden.pIsOverridden = pIsOverridden;
		addTraceMessage("decideIfEmprCntrbIsOverridden", FABM_METHODEXIT);
		takeBenchMark("decideIfEmprCntrbIsOverridden", FABM_METHODEXIT);
		return bvFABMOSCDecideIfEmprCntrbIsOverridden;	
	}
	/*********************************************************
	 * Method Name : RefundUnallocOSCCheck()
	 *
	 * Input Parameters :
	 *    P_BEMbrAcct : BE_Mbr_Acct
	 *    P_aoBVUnallocChks : array of BV_Unalloc_Chks
	 *	 P_RfndAmt : DecimalNullable
	 *
	 * Output Parameters :
	 *	None
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *       Refund unallocated checks to member
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 nyadav		08/22/2008  Migration from forte.
	 * NE.02					 jmisra		10/20/2008  Code modifed after code review.
	 *************************************************************************************/
	public void refundUnallocOSCCheck(BEMbrAcct pBEMbrAcct, List paoBVUnallocChks,
			BigDecimal pRfndAmt) throws ClaretyException {
		addTraceMessage("refundUnallocOSCCheck", FABM_METHODENTRY);
		startBenchMark("refundUnallocOSCCheck", FABM_METHODENTRY);
		List tempaoBVUnallocChks ;

		if(paoBVUnallocChks != null && paoBVUnallocChks.size() > JClaretyConstants.ZERO_INTEGER){
			tempaoBVUnallocChks = new ArrayList();
			tempaoBVUnallocChks.addAll(paoBVUnallocChks);
		}
		// validate input BEMbrAcct before crossing partition.  This also sets rcd_crt_nm, for use later.
		pBEMbrAcct.validateAll(getValidateHelper());
		//get rcd_crt_nm to pass into all newly read or created objects.
		String rcdCrtNm = pBEMbrAcct.rcd_crt_nm; 
		BEMbr beMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
		if(errorsPresent()){
			return ;
		}
		BEAddr beAddr ;
		if(beMbr!= null){
			beMbr.rcd_crt_nm = rcdCrtNm;
			Date curDate = getTimeMgr().getCurrentDateOnly();
			beAddr  = dbaddr.readByPart(beMbr);
			if(errorsPresent()){
				return ;
			}
		}else{
			addErrorMsg(56, new String[]{"Account", "member"});
			return;
		}//NE.02 - Begin
		if(beAddr == null || beAddr.addr_id == JClaretyConstants.ZERO_INTEGER){
			addErrorMsg(56, new String[]{"Address", "member"});
			return;
		}else{
			beAddr.rcd_crt_nm = rcdCrtNm;
		}
		BEPln bePln = dbpln.readByBusnAcct(pBEMbrAcct);
		if(errorsPresent()){
			return ;
		}
		if(bePln == null || bePln.pln_id == JClaretyConstants.ZERO_INTEGER ){
			addErrorMsg(56, new String[]{"Plan", "account"});
			return;
		}else{
			bePln.rcd_crt_nm = rcdCrtNm;
		}
		//NE.02 - End
//		Put together a BV_Chk_Dtl here.
		BEDocStatHist beDocStatHist = new BEDocStatHist();
		BVChkDtl bvChkDtl = new BVChkDtl();
		BigDecimal totUnallocAmount = JClaretyConstants.ZERO_BIGDECIMAL;
		totUnallocAmount.setScale(JClaretyConstants.FOUR_INTEGER);
		beDocStatHist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_E;
		beDocStatHist.doc_stat_dt = getTimeMgr().getCurrentDateOnly();
		beDocStatHist.rcd_crt_nm = rcdCrtNm;	

		bvChkDtl.addr = beAddr;
		bvChkDtl.busn_acct = pBEMbrAcct;
		bvChkDtl.pln = bePln;
		bvChkDtl.mbr = beMbr;
		bvChkDtl.doc_stat_hist = beDocStatHist;
		bvChkDtl.part = beMbr;
		bvChkDtl.rcd_crt_nm = rcdCrtNm;	

//		if P_RfndAmt <> Nil, then process refund from Voluntary Contributions Tab
		if( pRfndAmt != null){
			totUnallocAmount = pRfndAmt;
		}
		int count = JClaretyConstants.ZERO_INTEGER;
//		Build the From FncItems array
		List aoFromBEFncItems = new ArrayList();
		List tmpaoFromBEFncItems;
		if( paoBVUnallocChks != null){
			for(int i =0;i< paoBVUnallocChks.size();i++){
				BVUnallocChks row = (BVUnallocChks)paoBVUnallocChks.get(i);
				if( row.ao_be_fnc_item != null){	
					for(int j =0; j< row.ao_be_fnc_item.size();j++){
						aoFromBEFncItems.add(count,(BEFncItem)row.ao_be_fnc_item.get(j));
						((BEFncItem)aoFromBEFncItems.get(count)).rcd_crt_nm = rcdCrtNm;
						if(pRfndAmt== null){
						//NE.02 - Begin
							totUnallocAmount = totUnallocAmount.add(((BEFncItem)row.ao_be_fnc_item.get(j)).item_amt);
							totUnallocAmount = totUnallocAmount.subtract(((BEFncItem)row.ao_be_fnc_item.get(j)).from_alloc_amt);
						//NE.02 - End
						}
						count++;
					}
				}
				row.be_incmg_pymt.rcd_crt_nm = rcdCrtNm;
			}
		}
		BEOutgngPymt beOutgngPymt = new BEOutgngPymt();
		beOutgngPymt.chk_eft_dt = getTimeMgr().getCurrentDateOnly();
		beOutgngPymt.pye_typ_cd = JClaretyConstants.PYE_TYP_CD_MEM;
		beOutgngPymt.pymt_meth_cd = JClaretyConstants.PYMT_METH_CD_P;
		beOutgngPymt.pymt_net_amt = totUnallocAmount;
		beOutgngPymt.doc_typ_cd = JClaretyConstants.DOC_TYP_CD_MC;
		beOutgngPymt.doc_dt = getTimeMgr().getCurrentDateOnly();
		beOutgngPymt.rcd_crt_nm = rcdCrtNm;	
		beOutgngPymt.doc_fsc_yr = dbfscyr.calculateFiscalYearNr(bePln, beOutgngPymt.doc_dt);
		if(errorsPresent()){
			return ;
		}	      
		bvChkDtl.outgng_pymt = beOutgngPymt;
		// Create an array of financial items from the Unalloc_Checks.  This array will
		// have only one row
		List aoToBEFncItems = new ArrayList();
		BEFncItem toBEFncItems = new BEFncItem();
		toBEFncItems.from_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
		toBEFncItems.item_amt = totUnallocAmount;
		toBEFncItems.item_typ_cd = JClaretyConstants.ITEM_TYP_CD_NTMC;
		toBEFncItems.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
		toBEFncItems.rcd_crt_nm = rcdCrtNm;
		aoToBEFncItems.add(toBEFncItems);
		bvChkDtl.fnc_item_array = aoToBEFncItems;

		bvChkDtl = fabmcashdsbrs.saveOutgngPymt(bvChkDtl,
				false,
				null, null, null, null);
		if(errorsPresent()){
			return ;
		}
		BVFABMGLTransallocateItems bvFABMGLTransallocateItems =
			fabmgltrans.allocateItems(aoFromBEFncItems, aoToBEFncItems, null, null, null);
		if(errorsPresent()){
			return ;
		}
		beDocStatHist = new BEDocStatHist();
		beDocStatHist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_A;
		beDocStatHist.doc_stat_dt = getTimeMgr().getCurrentDateOnly();
		beDocStatHist.rcd_crt_nm = rcdCrtNm;	
		//Set the cash receipt status if fully allocated
		this.calculateAndSetCRStat(beDocStatHist,paoBVUnallocChks);
		if(errorsPresent()){
			return ;
		}     
		addTraceMessage("refundUnallocOSCCheck", FABM_METHODEXIT);
		takeBenchMark("refundUnallocOSCCheck", FABM_METHODEXIT);
		return ;	
	}
	/*********************************************************
	 * Method Name : calculateAndSetCRStat()
	 *
	 * Input Parameters :
	 *    P_BEDOcStatHist : BE_Doc_Stat_Hist
	 *    P_aoBVUnallocChks : Array of BV_Unalloc_Chks
	 *
	 * Output Parameters :
	 *	none
	 *
	 * Return Type :
	 *	ErrorStack
	 *
	 *
	 * Purpose :
	 *       This method will calculate if all items for a financial doc has been allocated.
	 *       If allocated then the fnc_doc status is set to allocated.
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 nyadav		08/22/2008  Migration from forte.
	 *************************************************************************************/
	public void calculateAndSetCRStat(BEDocStatHist pBEDocStatHist, List paoBVUnallocChks) throws ClaretyException {
		addTraceMessage("calculateAndSetCRStat", FABM_METHODENTRY);
		startBenchMark("calculateAndSetCRStat", FABM_METHODENTRY);
		// Check for the difference between the item amount and the From_Amount in each element
		// in the array of BE_Fnc_item in each BV_Unalloc_Chks.  If the difference is 0.0, then
		// all items have been allocated.  In this case set the status to allocated.
   
		BigDecimal diffAmount = JClaretyConstants.ZERO_BIGDECIMAL;
		if(paoBVUnallocChks!= null){
			for(int i =0; i<paoBVUnallocChks.size();i++){
				BVUnallocChks row = (BVUnallocChks)paoBVUnallocChks.get(i);
				BigDecimal tempResult = JClaretyConstants.ZERO_BIGDECIMAL;
				if(row.ao_be_fnc_item!= null){
					for(int j =0; j<row.ao_be_fnc_item.size();j++){
						BEFncItem item = (BEFncItem)row.ao_be_fnc_item.get(j);
						tempResult = item.item_amt.subtract(item.from_alloc_amt);
						diffAmount = diffAmount.add(tempResult);
					}
				}
				if(diffAmount.compareTo(JClaretyConstants.ZERO_BIGDECIMAL)== JClaretyConstants.ZERO_INTEGER){
					//Set incoming payment as allocated
					pBEDocStatHist.opt_lck_ctl = 0;	  	            
					row = fabmcashrcpts.setCashRcptStatForBVUnallocChk(row, pBEDocStatHist);                                                                 
					if(errorsPresent()){
						return ;
					} 
				}
      
			}
		}
		addTraceMessage("calculateAndSetCRStat", FABM_METHODEXIT);
		takeBenchMark("calculateAndSetCRStat", FABM_METHODEXIT);
		return ;	
	}

	/*********************************************************
	 * Method Name : findPyrlPyrdForMKUP()
	 *
	 * Input Parameters	:
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *	P_BEEmpr : BE_Empr
	 *	P_BEPln : BE_Pln
	 *	P_BEPyrl : BE_Pyrl
	 *	P_BEPstnHist : BE_Pstn_Hist
	 *	
	 * Output Parameters	:
	 *	P_aoBEPyrl : Array of BE_Pyrl
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method is used to build MKUP payroll period for DB plan based on the following 
	 *	rules:
	 *	1) If the Make Up contribution is Pre PIONEER then use the interest ref Table to 
	 *	build the payroll period. If Interest was posted annually means the payroll 
	 *	period is annually, if interest was posted quarterly then  payroll period is 
	 *	quarterly, if interest was posted monthly then payroll period was monthly.
	 *	2) If Make Up contribution is on or after PIONEER then use employer payroll period
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 nyadav		08/21/2008  Migration from forte.
	 * P.01		PIR 78840		sharman		02/25/2009	Modified a line to read array to size - 1 for 
	 * 													removing unwanted error "Start Date and End Date
	 * 													do not fall in pay period". and matched with forte.
	 * P.02     PIR 76777   	sharman		02/25/2009	modifed the code for QTRL pre-PIONEER payroll.
	 * P.03     PIR 77793       Lashmi      10/05/2009  For Pre-PIONEER make up records, quarterly payroll
	 *                                                  records are constructed even for ANNL int rate ref records 
	 *************************************************************************************/
	public List findPyrlPyrdForMKUP(BEMbrAcct pBEMbrAcct, BEEmpr pBEEmpr, BEPln pBEPln, BEPyrl pBEPyrl,
			BEPstnHist pBEPstnHist) throws ClaretyException {
		addTraceMessage("findPyrlPyrdForMKUP", FABM_METHODENTRY);
		startBenchMark("findPyrlPyrdForMKUP", FABM_METHODENTRY);
		List  aoBEPyrl = new ArrayList();
		Date rollOutDt = new Date();
		rollOutDt = getDateFormatter().stringToDateMMDDYYYY(JClaretyConstants.ROLL_OUT_DATE);
		List aoBEIntRateRef = null;
		Date tmpdt = new Date();
		tmpdt = getDateFormatter().stringToDateMMDDYYYY(JClaretyConstants.CLARETY_MAX_DATE_mmddyyyy);
		if((pBEPyrl.srv_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)) &&  
				(pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL || pBEPln.pln_id ==JClaretyConstants.PLN_ID_JUDGES 
						|| pBEPln.pln_id == JClaretyConstants.PLN_ID_PTRL || pBEPln.pln_id == JClaretyConstants.PLN_ID_JDGS_7)){
			if(!getDateCompare().isDayBefore(pBEPyrl.strt_dt, rollOutDt)){
				//pyrl.strt_dt >= roll_out_dt, meaning pyrl is in Post PIONEER
				//If it's a open position build payroll only until the current date
				if(getDateCompare().isSameDay(pBEPstnHist.pstn_end_dt, tmpdt)){
					tmpdt= getTimeMgr().getCurrentDateOnly();
				}else{
					tmpdt = pBEPstnHist.pstn_end_dt;
				}
				//Get all payroll periods for the start and end date of the position 
				//Read on FABMORG to get all aobepyrl
				aoBEPyrl = fabmorganization.buildPyrlForEmpr(pBEEmpr, pBEPstnHist.pstn_begin_dt, tmpdt);
				if(errorsPresent()){
					return null;
				}
			}else{
				//Pyrl is Pre-PIONEER date
				aoBEPyrl = new ArrayList();			
				int i = JClaretyConstants.ZERO_INTEGER;
				Date tmpEnddt = new Date();
				Date tmpPyrlEnddt = new Date();
				IntervalData tmpIntData = new IntervalData();
				tmpIntData.setUnit(0, 0, 1, 0, 0, 0, 0);
				BEPyrl tmpBEPyrl = new BEPyrl();
				BEFncDoc beFncDoc = new BEFncDoc();
				BEFscYr beFscYr;

				//Read the fiscal year for the plan
				beFscYr = dbfscyr.readByPln(pBEPln);
				if(errorsPresent()){
					return null;
				}
				if(beFscYr!= null){
					tmpEnddt = beFscYr.computeFsclEndDtOfDt(pBEPyrl.end_dt, getValidateHelper());
					if(errorsPresent()){
						return null;
					}
					if(tmpEnddt!= null){
						tmpEnddt = DateUtility.add(tmpEnddt, tmpIntData);
						//Add one day to this so that it points to next fiscal year
					}
				}
				//Read all the interest rates present less than the fical year end of the pyrl end date
				if(tmpEnddt!= null){
					aoBEIntRateRef = dbIntRateRef.readAllByEffDtNPlnIdNIntRateTypCd(tmpEnddt, pBEPln.pln_id, JClaretyConstants.INT_RATE_TYP_CD_REG);
					if(errorsPresent()){
						return null;
					}
				}
				//Find out the fiscal year end date in which the pyrl ends
				tmpPyrlEnddt = beFscYr.computeFsclEndDtOfDt(pBEPyrl.end_dt, getValidateHelper());
				if(errorsPresent()){
					return null;
				}
				if(tmpPyrlEnddt!= null){
					tmpPyrlEnddt = DateUtility.add(tmpPyrlEnddt, tmpIntData);
					//Add one day to this so that it points to next fiscal year
				}
				if(aoBEIntRateRef!= null && aoBEIntRateRef.size()>JClaretyConstants.ZERO_INTEGER){
					while( i < aoBEIntRateRef.size() && !getDateCompare().isDayAfter(((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt, pBEPstnHist.pstn_end_dt)){
						//Figure out the first element that has an eff date greater than the pstn start date
						while(!getDateCompare().isDayAfter(
								((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt, tmpPyrlEnddt)){ 
							//similar to aoBEIntRateRef[i].eff_dt <= P_BEPstnHist.pstn_begin_dt
							i = i + 1; //points to the next record
							//1M@P.01
							if( i >= aoBEIntRateRef.size() -1 || getDateCompare().isDayAfter(
									((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt, tmpPyrlEnddt)){
								i = i - 1;
								//point to one rate less than the position begin date
								break;
							}
						}
						
						if(((BEIntRateRef)aoBEIntRateRef.get(i)).frq_cd.trim().equals(JClaretyConstants.FRQ_CD_ANNL)){
							tmpBEPyrl.strt_dt = ((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt;
							tmpEnddt = beFscYr.computeFsclEndDtOfDt(tmpBEPyrl.strt_dt , getValidateHelper());
							if(errorsPresent()){
								return null;
							}
							if(tmpEnddt!= null && !(getDateCompare().isDayAfter(tmpEnddt, pBEPstnHist.pstn_end_dt))){
								//Pyrl end date cannot be greater than the position end date
								tmpBEPyrl.end_dt = tmpEnddt;
								BEPyrl newTmpBEPyrl =(BEPyrl)tmpBEPyrl.clone();
								aoBEPyrl.add(newTmpBEPyrl);
							}						
						}
						//1D@lashmi
						//else if(((BEIntRateRef)aoBEIntRateRef.get(i)).frq_cd.trim().equals(JClaretyConstants.FRQ_CD_QRTL))
						
						//A@lashmi starts
						if( ( ( (BEIntRateRef)aoBEIntRateRef.get(i)).frq_cd.trim().equals(JClaretyConstants.FRQ_CD_ANNL))
							|| ( ( (BEIntRateRef)aoBEIntRateRef.get(i)).frq_cd.trim().equals(JClaretyConstants.FRQ_CD_QRTL))){
						//A@lashmi ends	
							for(int j= 0; j<4;j++){ 
								//4 quarters in a year
								if(j==JClaretyConstants.ZERO_INTEGER){
									tmpBEPyrl.strt_dt = ((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt;
								}
								tmpEnddt = beFscYr.computeQtrEndDtOfDt(tmpBEPyrl.strt_dt, getValidateHelper());
								if(errorsPresent()){
									return null;
								}
								if(tmpEnddt!= null
										//1D@P.02
										//&& !getDateCompare().isDayAfter(tmpEnddt, pBEPstnHist.pstn_end_dt)){
									//Pyrl end date cannot be greater than the position end date
										 //1A@P.02
										&& getDateCompare().isDayOnOrBefore(tmpEnddt, pBEPstnHist.pstn_end_dt)){
									tmpBEPyrl.end_dt = tmpEnddt;
									BEPyrl newTmpBEPyrl =(BEPyrl)tmpBEPyrl.clone();
									aoBEPyrl.add(newTmpBEPyrl);
									tmpdt = tmpBEPyrl.end_dt;
									tmpdt = DateUtility.add(tmpdt, tmpIntData);
									tmpBEPyrl.strt_dt = tmpdt;
									//Set start date = End date + one day								
								}	
								//4A@P.02	
								else if ( tmpEnddt !=null ) {
									tmpBEPyrl.end_dt= pBEPyrl.end_dt;
									aoBEPyrl.add( tmpBEPyrl);
									break;	
								}

							}
						}else if(((BEIntRateRef)aoBEIntRateRef.get(i)).frq_cd.trim().equals(JClaretyConstants.FRQ_CD_MNLY)){
							for(int j= 0; j<12;j++){ 
								//12 months in a year
								if(j==JClaretyConstants.ZERO_INTEGER){
									tmpBEPyrl.strt_dt = ((BEIntRateRef)aoBEIntRateRef.get(i)).eff_dt;
								}
								Calendar calStDt = Calendar.getInstance();
								calStDt.setTime(tmpBEPyrl.strt_dt);
								int pMnthNr = calStDt.get(Calendar.MONTH)+1;
								int pYrNr = calStDt.get(Calendar.YEAR);								
								tmpEnddt =beFncDoc.getLastDayOfMnth(pMnthNr, pYrNr);
								
								if(errorsPresent()){
									return null;
								}
								if(tmpEnddt!= null && !(getDateCompare().isDayAfter(tmpEnddt, pBEPstnHist.pstn_end_dt))){
									//Pyrl end date cannot be greater than the position end date
									tmpBEPyrl.end_dt = tmpEnddt;
									BEPyrl newTmpBEPyrl =(BEPyrl)tmpBEPyrl.clone();
									aoBEPyrl.add(newTmpBEPyrl);
									tmpdt = tmpBEPyrl.end_dt;
									tmpdt = DateUtility.add(tmpdt, tmpIntData);
									tmpBEPyrl.strt_dt = tmpdt;
									//Set start date = End date + one day
								}	
							}

						}
						i = i + 1; //point to the next record	
					}	
				}

			}
		}
		if(aoBEPyrl != null && aoBEPyrl.size()==JClaretyConstants.ZERO_INTEGER){
			aoBEPyrl = null;
		}
		addTraceMessage("findPyrlPyrdForMKUP", FABM_METHODEXIT);
		takeBenchMark("findPyrlPyrdForMKUP", FABM_METHODEXIT);
		return aoBEPyrl;	
	}
	/**
	 * Purpose: Delete an OSC agreement.
	 * @param BVAgrmt
	 * @return void
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 vkundra     07/28/2008  Migration from forte.
	 * C.01	  CCR-77794			 jmisra      10/14/2009  Modified for CCC-77794.
	 * P.01		80782			 la			10/28/2009	 When a agreement is voided or deleted, all the contribution
	 * 													 that are in 'AGMT'/'PNDG' status, will be changed to 'RFND'
	 * 													 status.
	 * P.02		80782			gpannu		11/02/2009	 Changed the code as suggested by ramya.	
	 *************************************************************************************/
	public void deleteOSCAgrmt(BVAgrmt pBVAgrmt) throws ClaretyException {

		addTraceMessage("deleteOSCAgrmt", FABM_METHODENTRY);
		startBenchMark("deleteOSCAgrmt", FABM_METHODENTRY);

		List aoBVPyrlDtl = null;
		if(pBVAgrmt != null){
			aoBVPyrlDtl = dbfndgrqmt.readBVPyrlDtlByAgrmt(pBVAgrmt.agrmt, pBVAgrmt.mbr_acct.acct_id);
			if(errorsPresent()){
				return;
			}
			if(aoBVPyrlDtl != null && aoBVPyrlDtl.size() > JClaretyConstants.ZERO_INTEGER){
				for(int i=0; i<aoBVPyrlDtl.size(); i++){
					
					BVPyrlDtl row = (BVPyrlDtl)aoBVPyrlDtl.get(i);
					//P.01 starts
					if( pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CAT_CD_BRFD) ){ //1A@P.02

                        if(row.becntrb.stat_cd != null && (row.becntrb.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_PNDG)
                        		|| row.becntrb.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_AGMT))){
                                    dbcntrb.deleteCntrbStatUpToLastRfndStat(row.becntrb.cntrb_id);
                                    if (errorsPresent()) {
                                                return;
                                    }
                        }
                     } 

					//P.01 ends
					row.befndgrqmt.agrmt_id = 0;
					//Delink the funding rqmt from the agrmt & then save
					row.befndgrqmt = dbfndgrqmt.save(row.befndgrqmt, JClaretyConstants.ZERO_INTEGER);
					if(errorsPresent()){
						return;
					}
				}
			}
			if(pBVAgrmt.aobepymtinstr != null){
				for(int i=0; i<pBVAgrmt.aobepymtinstr.size(); i++){
					BEPymtInstr row = (BEPymtInstr)pBVAgrmt.aobepymtinstr.get(i);
					//Delete the payment instruction associated with the agreement
					dBPymtInstr.delete(row);
					if(errorsPresent()){
						return;
					}
				}
			}
			
			//9A@C.01
			if(pBVAgrmt.aobepymtinstrhstry != null){ //(2) CCR-77794
				for(int i=0; i<pBVAgrmt.aobepymtinstrhstry.size(); i++){
					BEEmprChngDtl row = (BEEmprChngDtl)pBVAgrmt.aobepymtinstrhstry.get(i);
					dbEmprChngDtl.delete(row);
					if(errorsPresent()){
						return;
					}
				}
			}
			
			dbagrmt.deleteAgrmt(pBVAgrmt.agrmt);
			if(errorsPresent()){
				return;
			}
			fabmgltrans.deleteFncDoc(pBVAgrmt.agrmt);
			if(errorsPresent()){
				//addErrorMsg(1023, new String[]{"Agreement cannot to Deleted"});
				return;
			}
		}else {
			addErrorMsg(1023, new String[]{"No Agreement was not passed in to be deleted"});
			return;
		}

		addTraceMessage("deleteOSCAgrmt", FABM_METHODEXIT);
		takeBenchMark("deleteOSCAgrmt", FABM_METHODEXIT);
	}

	/**
	 * Purpose: 
	 *	This method is used to determine the type of calculation used for selected service credit type.
	 *	If service credit type is LOA, determine if LOA is actuarial cost or interest based cost
	 *	Rule 1: LOA If a member is hired after July 01,1996 or rehired on or after July 01,1996
	 *	use actuarial cost calculation
	 *	Rule 2: LOA If a member is hired before July 01,1996 use interest calculation
	 *	Rule 3: Voluntary Refund school plan. If a member is re-employed for less than or
	 *	equal to 3 years from re-employment start date to refund  date use Interest Based
	 *	Rule 4: Voluntary Refund school plan. If the member is re-employed for more than
	 *	3 years from re-employment start date to refund date use ROR
	 *	Rule 5: Voluntary Refund and Mandatory Refund  for Plan 2,3,4,5 no calculation type
	 *	Rule 6: Voluntary Refund for Plan 2,3 use interest calculation
	 *	
	 *@param BEPln
	 *@param BEMbrAcct
	 *@param BESrvTypRef
	 *@param Date
	 *@param BVPyrlLOA
	 *@param List
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					 vkundra	   07/28/2008  Migration from forte.	
	 * P.01		NPRIS-5923	    vgp			07/29/2013	Fix refund buy back calc type
	 *************************************************************************************/
	public String detCalcTypeBySrvcCrType(BEPln pBEPln, BEMbrAcct pBEMbrAcct, 
			BESrvTypRef pBESrvTypRef, Date pRfnDt, BVPyrlLOA pBVPyrlLOA, List paoBVPyrlLOA) throws ClaretyException {

		addTraceMessage("detCalcTypeBySrvcCrType", FABM_METHODENTRY);
		startBenchMark("detCalcTypeBySrvcCrType", FABM_METHODENTRY);

		Assert.notNull("pBESrvTypRef", pBESrvTypRef);

		Date hireDt = getDateFormatter().stringToDateMMDDYYYY(JClaretyConstants.LOA_HIRE_DATE);
		IntervalData tmpIntData = new IntervalData();
		tmpIntData.setUnit(1, 0, 0, 0, 0, 0, 0);
		String pCalcType = "";
		BELOA bELOA = null;
		BEPyrl bEPyrl = null;
		Date cntrbDate = null;
		Date tempDate = null;
		Date apprvDate = null;
		boolean hireOrRehireAfter1996 = false;

		if(pBESrvTypRef.srv_crdt_cli_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOA)){
			//  BVPyrlLOA will always be passed for LOA cases.		   		   		   
			if(pBVPyrlLOA != null){
				List aoBEPyrl = null;
				//Get all non-reversed payroll records from member account. Loop through the records to find 
				//difference in days between the start date of one payroll to the start date of the next.
				//If a payroll is found that starts after the 19 Jun 1996 after a break of 180 days then 
				//the calculation type should be Actuarial.
				aoBEPyrl = dbpyrl.readFirstNLastPyrlByAcct(pBEMbrAcct);
				if(errorsPresent()){
					return null;
				}
				
				if(aoBEPyrl != null && aoBEPyrl.size() != JClaretyConstants.ZERO_INTEGER){
					//Plug in all other approved LOA
					if(paoBVPyrlLOA != null){
						for(int i=0; i<paoBVPyrlLOA.size(); i++){
							BVPyrlLOA row = (BVPyrlLOA)paoBVPyrlLOA.get(i);
							aoBEPyrl.add(row);
						}
					}
					//aoBEPyrl.sort('strt_dt');
					aoBEPyrl = QSortComparator.quickSort(aoBEPyrl, "strt_dt", "asc");
					IntervalData dayInterval = new IntervalData();
					dayInterval.setUnit(0, 0, 180, 0, 0, 0, 0);
					tempDate = new Date();
					for(int i=0; i<aoBEPyrl.size()-1; i++){
						BEPyrl row = (BEPyrl)aoBEPyrl.get(i);
						BEPyrl row1 = (BEPyrl)aoBEPyrl.get(i+1);
						tempDate = row.strt_dt;
						tempDate = DateUtility.add(tempDate, dayInterval);
						if(!(getDateCompare().isDayAfter(tempDate, row1.strt_dt)) && (getDateCompare().isDayAfter(row1.strt_dt,hireDt))){
							hireOrRehireAfter1996 = true;
							break;
						}
					}
				}
				if(hireOrRehireAfter1996){
					// If the hire date is greater than July 19 1996 
					pCalcType = JClaretyConstants.CALC_TYP_Actuarial;
				}else {
					bELOA = dBLOA.readByLoaId(pBVPyrlLOA.loa_id);
					if(errorsPresent()){
						return null;
					}
					if(bELOA != null){
						//Find the first contribution after LOA end date.
						bEPyrl = dbpyrl.readForNextCntrbPstng(pBEMbrAcct.acct_id, bELOA.end_dt);
						if(errorsPresent()){
							return null;
						}
						cntrbDate = new Date();
						if(bEPyrl != null){
							cntrbDate = bEPyrl.strt_dt;
						}else {
							cntrbDate = getTimeMgr().getCurrentDateOnly();
						}
						tempDate = new Date();
						IntervalData tempInterval = new IntervalData();
						tempInterval.addUnit(1, 0, 0, 0, 0, 0, 0);
						tempDate = DateUtility.add(bELOA.end_dt, tempInterval);
						if(getDateCompare().isDayBefore(tempDate, cntrbDate)){
							pCalcType = JClaretyConstants.CALC_TYP_Actuarial;
						}else {
							// Check for 3 years lapsed between first contribution and approve date.
							apprvDate = new Date();
							if(getDateCompare().isDayAfter(bELOA.aprvl_dt,cntrbDate)){
								apprvDate = bELOA.aprvl_dt;
							}else {
								apprvDate = getTimeMgr().getCurrentDateOnly();
							}
							tempDate = new Date();
							tempInterval = new IntervalData();
							tempInterval.addUnit(3, 0, 0, 0, 0, 0, 0);
							tempDate = DateUtility.add(cntrbDate, tempInterval);
							if(getDateCompare().isDayAfter(apprvDate, tempDate)){
								pCalcType = JClaretyConstants.CALC_TYP_Actuarial;
							}else {
								pCalcType = JClaretyConstants.CALC_TYP_Interest;
							}
						}
					}
				}
			}//Voluntary refund
		}else if(pBESrvTypRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)){
			if(pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL){
				/* The following logic should be used.
				  If from the last most current employment  3 years has passed then the calculation type is ROR.
				  If from the last most current employment  to current date 3 years has not passed then Interest way.*/

				BEMbr bEMbr = null;
				List aoBEEmptHist = null;
				Date crntDt = getTimeMgr().getCurrentDateOnly();
				bEMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
				if(errorsPresent()){
					return null;
				}
				aoBEEmptHist = dbempthist.readAllEmptForMbrAcctWODummyOrg(pBEMbrAcct);
				if(errorsPresent()){
					return null;
				}
				if(aoBEEmptHist == null || aoBEEmptHist.size() == JClaretyConstants.ZERO_INTEGER){
					addErrorMsg(1035, new String[]{"Employment details does not exist to determine Calculation Type"});
					return JClaretyConstants.CLARETY_EMPTY_FIELD;
				}else {
					/*Do not go by the last employment. It might so happen that
					 the mbr switched schools or the employer merged recently though
					 the mbr has been contributing for more than 3 years*/

					//remove the employments prior to refund date //P.01: remove all past employment 
					for(int i=aoBEEmptHist.size()-1; i>=0; i--){
						BEEmptHist row = (BEEmptHist)aoBEEmptHist.get(i);
						if(getDateCompare().isDayOnOrBefore(row.end_dt,pRfnDt)){
							aoBEEmptHist.remove(i);
						}
					}
					//Take the first employment after refund and check the date
					if (aoBEEmptHist != null
							&& aoBEEmptHist.size() > JClaretyConstants.ZERO_INTEGER) {
						aoBEEmptHist = QSortComparator.quickSort(aoBEEmptHist,
								"strt_dt", "asc");
						IntervalData dateInterval = new IntervalData();
						dateInterval.addUnit(3, 0, 0, 0, 0, 0, 0);
						// A@P.01 End
					
						for (int i = 0; i < aoBEEmptHist.size(); i++) {
							BEEmptHist beEmptHist = (BEEmptHist) aoBEEmptHist
									.get(i);
							if (getDateCompare().isDayOnOrAfter(
									beEmptHist.end_dt, crntDt)) {
								pCalcType = JClaretyConstants.CALC_TYP_Interest; // default, break loop after set to ROR
								if (getDateCompare().isDayBefore(
										DateUtility.add(beEmptHist.strt_dt,
												dateInterval), crntDt)) {
									pCalcType = JClaretyConstants.CALC_TYP_ROR;
									break;
								} else {
									// check for merged employment
									List<BEEmptHist> aoMergeBEEmptHist = dbempthist
											.readRelatedMergedEmpt(pBEMbrAcct,
													beEmptHist);
									if (aoMergeBEEmptHist != null
											&& aoMergeBEEmptHist.size() > JClaretyConstants.ZERO_INTEGER) {
										// list is in asc order of strt_dt
										if (getDateCompare()
												.isDayBefore(
														DateUtility.add(
																aoMergeBEEmptHist
																		.get(0).strt_dt,
																dateInterval),
														crntDt)) {
											pCalcType = JClaretyConstants.CALC_TYP_ROR;
											break;
										}
									}
								}
							}
						}
						//A@P.01 End
						//D@P.01 Start
//						((BEEmptHist)aoBEEmptHist.get(0)).strt_dt = DateUtility.add(((BEEmptHist)aoBEEmptHist.get(0)).strt_dt, dateInterval);
//						//Let the CrntDt be 07-Nov-2002 and Strt_dt be 01-Jul-1998. Add 3 years to Strt_dt,
//						//it becomes 01-Jul-2001,which means he is employed for more than 3 years. So CalcType
//						// is ROR based.
//						if(getDateCompare().isDayBefore(((BEEmptHist)aoBEEmptHist.get(0)).strt_dt, crntDt)){
//							pCalcType = JClaretyConstants.CALC_TYP_ROR;
//						}else {
//							pCalcType = JClaretyConstants.CALC_TYP_Interest;
//						}
						//D@P.01 End
					}else {
						//No Employments found after Refund
						addErrorMsg(1035, new String[]{"Employment details does not exist to determine Calculation Type"});
						return JClaretyConstants.CLARETY_EMPTY_FIELD;
					}
				}
			}else if(pBEPln.pln_id == JClaretyConstants.PLN_ID_JUDGES || 
					pBEPln.pln_id == JClaretyConstants.PLN_ID_PTRL || 
					pBEPln.pln_id == JClaretyConstants.PLN_ID_JDGS_7) {
				//Judges & Patrol plan
				pCalcType = JClaretyConstants.CALC_TYP_Interest;
			}else {
				// for DC plans do nothing return null
			}
		}

		addTraceMessage("detCalcTypeBySrvcCrType", FABM_METHODEXIT);
		takeBenchMark("detCalcTypeBySrvcCrType", FABM_METHODEXIT);
		return pCalcType;
	}


	/**
	 * Purpose: Given a new payroll record, see if it will overlap with any existing payroll for the member.  Returns a string containing the period of overlap so that tab can display it to the user.
	 * @param BEMbrAcct
	 * @param BEPyrl
	 * @return void
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					  vkundra	   07/29/2008  Migration from forte. 
	 *************************************************************************************/
	public void findOverlapBetweenPayroll(BEMbrAcct pBEMbrAcct, BEPyrl pBEPyrl)	throws ClaretyException {

		addTraceMessage("findOverlapBetweenPayroll", FABM_METHODENTRY);
		startBenchMark("findOverlapBetweenPayroll", FABM_METHODENTRY);

		Assert.notNull("pBEPyrl", pBEPyrl);

		int startYear = pBEPyrl.fscl_yr_strt_nr;
		int endYear = pBEPyrl.fscl_yr_end_nr;
		List aoBEPyrl = null;
		boolean overlapFound = false;

		for(int i=startYear; i<=endYear; i++){
			aoBEPyrl = dbpyrl.readPyrlForYr(pBEMbrAcct, i);
			if(errorsPresent()){
				return;
			}
			if(aoBEPyrl != null && aoBEPyrl.size() > JClaretyConstants.ZERO_INTEGER){
				for(int j=0; j<aoBEPyrl.size(); j++){
					BEPyrl row = (BEPyrl)aoBEPyrl.get(j);
					if(errorsPresent()){
						overlapFound = true;
						break;
					}
					pBEPyrl.validateNonOverlapOfExistingDateRange(row.strt_dt, row.end_dt, pBEPyrl.strt_dt, pBEPyrl.end_dt);
					if(errorsPresent()){
						return ;
					}

				}//end for
			} // aoBEPyrl <> NIL.  It's okay for it to be NIL, so no errors
			else{
				break; // exit for loop if errors found.
			}
			if (overlapFound){
				break;
			}
		}//end for

		addTraceMessage("findOverlapBetweenPayroll", FABM_METHODEXIT);
		takeBenchMark("findOverlapBetweenPayroll", FABM_METHODEXIT);
	}


	/**
	 * Purpose: instantiate All DB's and FABM's
	 * @return 
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * PreDev                   nyadav       08/01/2008  Initial migration from Forte
	 * P.01		NPRIS-5564		vgp			01/10/2012	 Get DBBusnDt instance.
	 * P.02		NPRIS-6481		VenkataM	10/12/2017	 Get DBAcctTrans instance.
	 *************************************************************************************/
	public void init() throws ClaretyException {
		dbpln = (DBPln) getDBInstance(DBPln.class);
		dbLateIntPstgRpt = (DBLateIntPstgRpt) getDBInstance(DBLateIntPstgRpt.class);
		dbIntRateRef = (DBIntRateRef) getDBInstance(DBIntRateRef.class);
		dBPlnRef = (DBPlnRef) getDBInstance(DBPlnRef.class);
		dbaccttrans = (DBAcctTrans) getDBInstance(DBAcctTrans.class);		
		dbagrmt = (DBAgrmt) getDBInstance(DBAgrmt.class);
		dbcntrb = (DBCntrb) getDBInstance(DBCntrb.class);
		dbcostschd = (DBCostSchd) getDBInstance(DBCostSchd.class);
		dbdocstathist = (DBDocStatHist) getDBInstance(DBDocStatHist.class);
		dbdocstatref = (DBDocStatRef) getDBInstance(DBDocStatRef.class);
		dbempr = (DBEmpr) getDBInstance(DBEmpr.class);		
		dbempthist = (DBEmptHist) getDBInstance(DBEmptHist.class);
		dbfncalloc = (DBFncAlloc) getDBInstance(DBFncAlloc.class);
		dbfncitem = (DBFncItem) getDBInstance(DBFncItem.class);
		dbfndgrqmt = (DBFndgRqmt) getDBInstance(DBFndgRqmt.class);
		dbfscyr = (DBFscYr) getDBInstance(DBFscYr.class);
		dbintitem = (DBIntItem) getDBInstance(DBIntItem.class);   
		dbmbr = (DBMbr) getDBInstance(DBMbr.class);
		dbmbracct = (DBMbrAcct) getDBInstance(DBMbrAcct.class);
		dborg = (DBOrg) getDBInstance(DBOrg.class);
		dboscrqst = (DBOSCRqst) getDBInstance(DBOSCRqst.class);
		dbpart = (DBPart) getDBInstance(DBPart.class);
		dbpyrl = (DBPyrl) getDBInstance(DBPyrl.class);
		dbsrvcrdtref = (DBSrvCrdtRef) getDBInstance(DBSrvCrdtRef.class);
		fabmgltrans = (FABMGLTrans) getFABMInstance(FABMGLTrans.class);
		fabmmbracctmaint = (FABMMbrAcctMaint) getFABMInstance(FABMMbrAcctMaint.class);
		fabmorganization = (FABMOrganization) getFABMInstance(FABMOrganization.class);
		dbircmaxamtref = (DBIRCMaxAmtRef) getDBInstance(DBIRCMaxAmtRef.class);
		dBPymtInstr = (DBPymtInstr)getDBInstance(DBPymtInstr.class);
		dBLOA = (DBLOA)getDBInstance(DBLOA.class);
		dbPrcs = (DBPrcs)getDBInstance(DBPrcs.class);
		dbPymtInstr = (DBPymtInstr) getDBInstance(DBPymtInstr.class);
		dbRetSys = (DBRetSys) getDBInstance(DBRetSys.class);
		dbWCAcctPybl = (DBWCAcctPybl) getDBInstance(DBWCAcctPybl.class);
		dbaddr = (DBAddr) getDBInstance(DBAddr.class);
		fabmwageandcntrb = (FABMWageAndCntrb) getFABMInstance(FABMWageAndCntrb.class);
		fabmcashdsbrs = (FABMCashDsbrs)getFABMInstance(FABMCashDsbrs.class);
		fabmcashrcpts = (FABMCashRcpts)getFABMInstance(FABMCashRcpts.class);
		dbRfndReq = (DBRfndReq) getDBInstance(DBRfndReq.class);
		dbMkupPstng = (DBMkupPstng)getDBInstance(DBMkupPstng.class);
		dbEmprChngDtl = (DBEmprChngDtl) getDBInstance(DBEmprChngDtl.class);
		dBBusnDt = (DBBusnDt) getDBInstance(DBBusnDt.class);	//1A@P.01
		dbAcctTrans = (DBAcctTrans) getDBInstance(DBAcctTrans.class);//1A@P.02
	}

	/*  Purpose:
	 *	This method for Agrmts (if provided in aoBVAgrmt or for all agrmts in the member account) 
	 *	and finds the balance in each of the agreement in the member account
	/*
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01                     djain       02/06/2008   initial mgration from forte
	 * P.01		80305			 kgupta		 07/17/2009		Modified code to resolve issue of not reintiating object in loop.
	 * ***********************************************************************************/
	public BVOSCBal readAgrmtBalByAcctNEffDtNCtgry(BEMbrAcct pBEMbrAcct, BEPln pBEPln,String pCtgry, Date pEffDt, List paoBVAgrmt)
	throws ClaretyException {
		addTraceMessage("readAgrmtBalByAcctNEffDtNCtgry",FABM_METHODENTRY);
		takeBenchMark("readAgrmtBalByAcctNEffDtNCtgry",FABM_METHODENTRY);
		BEMbr beMbr = null;
		List aoBVAgrmt, tmpaoBVAgrmt, aoBEAcctTrans;//, aoBVOSCBal = new ArrayList();
		aoBVAgrmt = new ArrayList();
		tmpaoBVAgrmt = new ArrayList();
		//1D@P.01
		//BVOSCBal bvoscBal = new BVOSCBal();
		BVOSCBal pBVOSCBal = new BVOSCBal();
		BigDecimal OSCPreTax, OSCPostTax, refund_pre_tax, refund_post_tax = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal osc_int_pre_tax, osc_int_post_tax, tmp_refund_pre_tax, tmp_refund_post_tax, trans_pre_tax_amt, trans_post_tax_amt = JClaretyConstants.ZERO_BIGDECIMAL;
		Boolean refund_exists;
		BigDecimal RfndSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(JClaretyConstants.FOUR_INTEGER);
		BEDocStatRef beDocStatRef = null;
		//Read member by member account
		beMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
		if (errorsPresent()) {
			return null;
		}
		//Select ALL agreements by member account -> Returns array of BV_Agrmt (prorated, completed, in process defereed for IRC415)	
		if (paoBVAgrmt == null) {
			if (beMbr != null && pBEPln != null) {
				tmpaoBVAgrmt = (ArrayList) dbagrmt.readByMbrNPlnNOrgNStatGrp(beMbr,pBEPln, null, "All", pEffDt);//stat_grp_cd = All by default
				if (errorsPresent()) {
					return null;
				}
			}
		}
		else {
			tmpaoBVAgrmt = paoBVAgrmt;
		}
		if (tmpaoBVAgrmt != null) {
			if (pCtgry.trim().equals(JClaretyConstants.CTGRY_PSTD)) {//Called from EFABM_MAM
				int size = tmpaoBVAgrmt.size();
				for (int i = 0; i < size; i++) {
					//consider only Completed & Pro-rated agrmts
					BVAgrmt bvAgrmt = (BVAgrmt) tmpaoBVAgrmt.get(i);
					bvAgrmt.agrmt_stat_cd = bvAgrmt.agrmt_stat_cd.trim();
					if (bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_C) ||
							bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_R) ||
							bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_AC)||
							bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_AR)) {
						aoBVAgrmt.add(tmpaoBVAgrmt.get(i));
					}
				}
			}
			else {//Called by Refunds
				aoBVAgrmt = tmpaoBVAgrmt;
			}
			int size = aoBVAgrmt.size();
			for (int i = 0; i < size; i++) {
			//1A@P.01
				BVOSCBal bvoscBal = new BVOSCBal();
				BVAgrmt bvAgrmt = (BVAgrmt) aoBVAgrmt.get(i);
				refund_exists = Boolean.FALSE;
				refund_pre_tax = JClaretyConstants.ZERO_BIGDECIMAL;
				refund_post_tax = JClaretyConstants.ZERO_BIGDECIMAL;
				if (pCtgry.trim().equals(JClaretyConstants.CTGRY_PSTD)) {//Called from EFABM_MAM
					//Read acct_trans attached to the agreement
					aoBEAcctTrans = (ArrayList) dbaccttrans.readByAgrmt(bvAgrmt.agrmt);
					if (errorsPresent()) {
						return null;
					}
					//If account trans returned is nil, it means it has been reversed, so ignore this agreement
					//The SQL in the above method ReadByAgrmt choses acct trans with rvrs_trans_id as null

					//-- For airtime Account trans can be nil for a posted agreement 
					if (aoBEAcctTrans != null) {
						// Check to see whether agreement is closed or not
						fabmmbracctmaint.checkForFutureNonRvrsdClosure(pBEMbrAcct,(BEAcctTrans) aoBEAcctTrans.get(0),false);
						if (errorsPresent()) {//Nonreversed Closure exists
							continue;
						}
						if (bvAgrmt.berfndreq != null) {
							//Read account transaction by rfnd_req
							aoBEAcctTrans = (ArrayList) dbaccttrans.readByRfndReq(bvAgrmt.berfndreq,pBEMbrAcct);
							if (errorsPresent()) {
								return null;
							}
							if (aoBEAcctTrans != null) {
								refund_exists = Boolean.TRUE;
								int size1 = aoBEAcctTrans.size();
								for (int j = 0; i < size1; j++) {
									//new method gives refund_pre_tax.value & refund_post_tax.value 
									BVFndgRqmt bvFndgRqmt = dbfndgrqmt.sumByAcctTrans(((BEAcctTrans) aoBEAcctTrans.get(j)));
									if (errorsPresent()) {
										return null;
									}
									tmp_refund_pre_tax = bvFndgRqmt.pretaxamt;
									tmp_refund_post_tax = bvFndgRqmt.posttaxamt;
									osc_int_pre_tax = bvFndgRqmt.oscintpretaxamt;
									osc_int_post_tax = bvFndgRqmt.oscintposttaxamt;
									trans_pre_tax_amt = bvFndgRqmt.trans_pre_tax;
									trans_post_tax_amt = bvFndgRqmt.trans_post_tax;
									refund_pre_tax = refund_pre_tax.add(tmp_refund_pre_tax);
									refund_pre_tax = refund_pre_tax.add(osc_int_pre_tax);
									refund_post_tax = refund_post_tax.add(tmp_refund_post_tax);
									refund_post_tax = refund_post_tax.add(osc_int_post_tax);
									RfndSrv = RfndSrv.add(((BEAcctTrans) aoBEAcctTrans.get(j)).srv_crdt_net_amt);
								}//end for
							}
						}
					}

				}//end if
				OSCPreTax = bvAgrmt.agrmt.paid_pre_tax_amt;
				OSCPostTax = bvAgrmt.agrmt.paid_post_tax_amt;
				if (refund_exists.booleanValue()) {
					OSCPreTax = OSCPreTax.add(refund_pre_tax);// since refund amount is negative, just add
					OSCPostTax = OSCPostTax.add(refund_post_tax);// since refund amount is negative, just add
				}
				bvoscBal.bvagrmt = bvAgrmt;
				beDocStatRef = dbdocstatref.readByGrpTypNStatCd(JClaretyConstants.DOC_GRP_TYP_CD_OSC,bvAgrmt.agrmt_stat_cd.trim());
				if(errorsPresent()){
					return null;
				}
				if (beDocStatRef.doc_stat_grp_cd.trim().equals(JClaretyConstants.DOC_STAT_GRP_CD_COMP)) {
					bvoscBal.oscpretax = OSCPreTax;
					bvoscBal.oscposttax = OSCPostTax;
				}
				else {
					bvoscBal.oscpndgpretax = OSCPreTax;
					bvoscBal.oscpndgposttax = OSCPostTax;
				}
				//keep adding to the top level BVOSCbal - o/p parameter
				pBVOSCBal.aobvoscbal.add(bvoscBal);
				if (beDocStatRef.doc_stat_grp_cd.trim().equals(JClaretyConstants.DOC_STAT_GRP_CD_COMP)) {
					// changes done as per forte - skumar4
					pBVOSCBal.oscpretax = pBVOSCBal.oscpretax.add(bvoscBal.oscpretax);
					pBVOSCBal.oscposttax = pBVOSCBal.oscposttax.add(bvoscBal.oscposttax);
				}
				else {
					// changes done as per forte - skumar4
					pBVOSCBal.oscpndgpretax = pBVOSCBal.oscpndgpretax.add(bvoscBal.oscpndgpretax);
					pBVOSCBal.oscpndgposttax = pBVOSCBal.oscpndgposttax.add(bvoscBal.oscpndgposttax);
				}
			}//end for
		}//end if
		addTraceMessage("readAgrmtBalByAcctNEffDtNCtgry",FABM_METHODEXIT);
		takeBenchMark("readAgrmtBalByAcctNEffDtNCtgry",FABM_METHODEXIT);
		return pBVOSCBal;
	}//method ends


	/*  Purpose:
	 	This method save the agreement , payment instructions, and call Fabm G/L to 
	 *	 save(fnc_doc,doc stat hist,)
	 *	1) If the agreement is a new agreement this method also links the agreement 
	 *	 to the fnd_rgmt depending the type of service credit. 
	 *		A)If the service credit is calculated actuarial way (LOAA, OOS, Air time) 
	 *		B) If the service credit is LOAI then this method links the funding requirement 
	 *		 to the agreement
	 *		C) If the service credit is Make Up contribution, since there is no an osc 
	 *		 request attached to a Make Up contribution, the method links the unfubded 
	 *		 fndg_rqmt to the agreement
	 *		D) If the service credit is Refund the it looks if the fndg_rqmt needs to be 
	 *		 linked for all the refunds or for the refunds selected (pick and choose)
	 *	2) If the agreement is done via MAM then changes the status Maintain Completed 
	 *	 in order for FABM G/L not to do G/L transaction
	 *	3) If the agreement is completed then it calls the Post ServiceCredit method to 
	 *	 recalculate interest and call MAM for OSC TransClosure
	/*
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.0                     asingh2     04/01/2008  Initial conversion from Forte to java
	 * NE.01					jmisra		11/04/2008	Modified for fixing NP Exception on tabPaymtn.
	 * P.01   79484             vsenthil    04/10/2009  Fixed the Pioneer code conversion issues
	 * C.01   77794             jmisra      10/14/2009  Modified for CCR-77794.
	 * C.02   77794             jmisra      11/02/2009  Modified for CCR-77794.
	 * C.03   77794             jmisra      11/04/2009  Modified for conditionally setting effective date.
	 * P.02   77795             gpannu      10/25/2009  Added code to create a AR for Employer if 
	 *                                                  agrmt is prorated/completed.  
	 * C.04	  77794				jmisra		11/18/2009	Modified to allow the user to change only eff_dt also 
	 * 													for current/former employer.(To fix the defect raised after UAT.)
	 * P.03   77795             singhp      11/19/2009  Fix for adjusting tot_rqd_amt and er_share.
	 * P.04   77795             singhp      11/24/2009  Fix to adjust last fndg rqmt tot_rqd_amt and er_share.
	 * P.05   77795             singhp      12/03/2009  Fix for paid pre tax or paid post tax not getting updated after AR creation.
	 * P.06   77795             singhp      12/24/2009  Fix for creating account receivables for Make up cases only.
	 * P.07	NPRIS-1194			gpannu		12/29/2009  Code added to include transaction on basis of monthly cntrb too.
	 * 													because prior this ccr transactions were getting selected for whole fsycl yr
	 *													while now with this CCR user can even select or unselect mnthly 
	 *													cntrbs also within any fyscl yr. 
	 * P.08   77795			   singhp	   02/08/2010  Fix for stopping AR Creation if within Tolerance amt
	 * P.09   77795			   singhp	   02/09/2010  Added check for Agreement completion.
	 * P.10   77795			   singhp	   02/10/2010  Added check for Make Up Case.	
	 * P.11	NPRIS-1194	       gpannu		02/19/2010  added code to save srv_stat_hist if saving the agrmnt for first time.
	 * P.12	NPRIS-5162		   la			05/07/2010	implemented condition if parent row is unchecked, then none of the child rows are selected.
	 * 													Since cost calc tab work in this ways, abv condition will hold true
	 * P.13	NPRIS-5209			vgp			07/30/2010	Get the empr org_id from pymnt_instr intead of cost_schd
	 * P.14	NPRIS-5485			vgp			02/07/2012	Do not allow any tolerance for EPYR and IPYR portions of MKUP
	 * P.15	NPRIS-6486			VenkataM	09/18/2017	Removed code as the beEmpyrChgDtl is not being saved and avoid 'continue' to save pymnt_instr
     * P.16	NPRIS-6481			VenkataM	01/04/2018	Added code to display error message for requirements #9, #10
	 *****************************************************************************************************************/
	public BVFabmOscSaveAgrmt saveAgrmt(BEPln pBEPln,Date pEffDt, BEOSCRqst pBEOSCRqst,boolean pTabMaintain, Date pPstngDt,
			BVAgrmt pBVAgrmt, boolean pHasPymtInstrChngd,List aoBVYrlyRfndTransChosen, boolean pPMA,
			boolean pHideTransIn) throws ClaretyException {
		
		startBenchMark("saveAgrmt", FABM_METHODENTRY);
		addTraceMessage("saveAgrmt", FABM_METHODENTRY);
		
		BVFabmOscSaveAgrmt bvFabmOscSaveAgrmt = new BVFabmOscSaveAgrmt();
		boolean isNewAgrmt = false;
		BVAgrmt oldBVAgrmt = null;
		BEOrg beOrg = null;
		BEMbr beMbr = null;
		BEFncDoc beFncDoc = null;
		BEFncItem beFncItem = null;
		BEPymtInstr bePymtInstr = null;		
		List tmpaoBVFndgRqmtCntrb = null;
		List aoBVFndgRqmtCntrb = new ArrayList();;
		BESrvCrdtRef beSrvCrdtRef = null;
		BVYrlyRfndTrans bvYrlyRfndTrans = null;
		BigDecimal srvCrdtToBuy = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);		
		BVOSCRqstInfo bvOSCRqstInfo = null;
		BEDocStatHist beDocStatHist = new BEDocStatHist();
		BEDocStatRef tempdocstatref = null;
		BEPlnRef beplnRef = null;
		BigDecimal preTaxAmt=JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal postTaxAmt=JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal ratio=JClaretyConstants.ZERO_BIGDECIMAL;	
		boolean createRcvbl = true;//4A@P.08
		if(pBVAgrmt != null && pBVAgrmt.agrmt_stat_cd != null//3AP.10 
				&& pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C)
				&& pBVAgrmt.agrmt.srv_crdt_typ != null 
				&& pBVAgrmt.agrmt.srv_crdt_typ.trim().equalsIgnoreCase(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)){//1M@P.09
		createRcvbl = pBVAgrmt.createRcvbl;
		}
		BVFndgRqmtCntrb tempFndgRqmtCntrb = null;
		////////////////////////////////////Some pre processing ////////////////////////////////////////////
		if (pBEPln != null) {
			beplnRef = dBPlnRef.readByPlan(pBEPln, pEffDt);
			if (errorsPresent()) {
				return null;
			}
		}
		if (pBEOSCRqst != null) {
			if (pBEOSCRqst instanceof BVOSCRqstInfo) {
				bvOSCRqstInfo = (BVOSCRqstInfo) pBEOSCRqst;
			}
			else {
				bvOSCRqstInfo = new BVOSCRqstInfo();
				bvOSCRqstInfo.setParentAttribute(pBEOSCRqst);
			}
		}
		if (pBVAgrmt.agrmt.opt_lck_ctl != JClaretyConstants.ZERO_INTEGER) {
			oldBVAgrmt = dbagrmt.readByAgrmtId(pBVAgrmt.agrmt.agrmt_id);
			if (errorsPresent() || oldBVAgrmt == null) {
				addErrorMsg(155, new String[]{"Agreement"});
				return null;
			}			
		}
		else {
			isNewAgrmt = true;
		}
		//A@P.13 start
		List aoBVPymtInstr = null;
		if(pBVAgrmt != null && pBVAgrmt.agrmt != null && pBVAgrmt.agrmt.srv_crdt_typ.trim().equalsIgnoreCase(JClaretyConstants.SRV_TYP_CD_MKUP)){
			aoBVPymtInstr = dbPymtInstr.readByAgrmtId(pBVAgrmt.agrmt);
			if(errorsPresent()){
				return null;
			}
		
			if(aoBVPymtInstr != null && aoBVPymtInstr.size() > JClaretyConstants.ZERO_INTEGER){
				for(int i = 0; i < aoBVPymtInstr.size(); i++){
					BVPymtInstr bvPymtInstr = (BVPymtInstr)aoBVPymtInstr.get(i);
					if (bvPymtInstr != null
							&& bvPymtInstr.bepymtinstr != null
							&& (bvPymtInstr.bepymtinstr.pymt_typ.trim().equalsIgnoreCase(JClaretyConstants.PYMT_TYP_EPYR) 
									|| bvPymtInstr.bepymtinstr.pymt_typ.trim().equalsIgnoreCase(JClaretyConstants.PYMT_TYP_IPYR))
							&& bvPymtInstr.bepymtinstr.pyr_id != JClaretyConstants.ZERO_INTEGER) {
						beOrg = dborg
								.readByOrgId(bvPymtInstr.bepymtinstr.pyr_id);
						if(errorsPresent()){
							return null;
						}
						break;
					}
				}
			}
		}
		//A@P.13 end
		//1M@P.13
		if (beOrg == null && pBVAgrmt.cost_schd.org_id != JClaretyConstants.ZERO_INTEGER
				&& pBVAgrmt.cost_schd.org_id > JClaretyConstants.ZERO_INTEGER) {
			beOrg = dborg.readByCostSchd(pBVAgrmt.cost_schd);
			if (errorsPresent()) {
				return null;
			}
			if (beOrg == null || beOrg.org_id == JClaretyConstants.ZERO_INTEGER) {
				//for Batch doubt
				addErrorMsg(0, new String[]{"Organization"});
				return null;
			}
		}

		bembr = dbmbr.readByMbrAcct(pBVAgrmt.mbr_acct);
		if (errorsPresent()) {
			return null;
		}
		if (bembr == null || bembr.mbr_id == JClaretyConstants.ZERO_INTEGER) {
			addErrorMsg(56, new String[]{"Member", "account"});
			return null;
		}
		
		if(pBEOSCRqst != null && pBEPln != null){
		if(pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL && pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)){
			validateCostCalcPymntDates(pBVAgrmt.mbr_acct, null,pBVAgrmt.agrmt.pymt_end_dt);
		}
		}

		//////////////////////////////////// Start save process ////////////////////////////////////////////
		// 	Save fnc doc, doc stat, agreement and payment instruction 
		////////////////////////////////////////////////////////////////////////////////////////////////////


		/*-----Building BEFncDoc to pass to SaveFncDoc-----*/
		befncdoc = pBVAgrmt.agrmt;
		/*-----Building BEDocStatHist to pass to SaveFncDoc-----*/
		beDocStatHist = new BEDocStatHist();
		if (!isNewAgrmt) {
			if (pBVAgrmt.agrmt_stat_cd.trim().equals(oldBVAgrmt.agrmt_stat_cd.trim())) {
				BEDocStatHist oldBEDocStatHist;
				oldBEDocStatHist = dbdocstathist.readCrntStatByFncDoc(oldBVAgrmt.agrmt);
				if (errorsPresent()) {
					return null;
				}
				if (oldBEDocStatHist == null || oldBEDocStatHist.doc_stat_hist_id == JClaretyConstants.ZERO_INTEGER) {
					addErrorMsg(1023, new String[]{" Current Status for the agreement not found "});
				}
				else {
					beDocStatHist.doc_stat_dt = oldBEDocStatHist.doc_stat_dt;
				}
			}
			else {
				beDocStatHist.doc_stat_dt = pEffDt;
			}
		}
		else {
			beDocStatHist.doc_stat_dt = pEffDt;
		}
		//When money is not really allocated, change the status to Maintain Complete
		boolean realMoney = false;
		if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R) ||
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C)) {
			List tp_aoBEFncAlloc = null;
			BEFncItem tempFncItem = null;
			int aoBEPymtInstrSize = pBVAgrmt.aobepymtinstr.size();
			int size = aoBEPymtInstrSize;
			for (int i = 0; i < size; i++) {
				bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(i);
				if (bePymtInstr.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
					tempFncItem = new BEFncItem();
					tempFncItem.fnc_item_id = bePymtInstr.fnc_item_id.intValue();
					tempFncItem = dbfncitem.readByFncItem(tempFncItem);
					if (errorsPresent()) {
						return null;
					}
					if (tempFncItem != null) {
						tp_aoBEFncAlloc = dbfncalloc.readByToItem(tempFncItem);
						if (errorsPresent()) {
							return null;
						}
						if (tp_aoBEFncAlloc != null && tp_aoBEFncAlloc.size() > JClaretyConstants.ZERO_INTEGER) {
							realMoney = true;
							break;
						}
					}
				}
			}
			if (realMoney == false) {
				if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R)) {
					pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_MR;
				}
				else {
					pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_MC;
				}
			}
			else if (pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)) {
				if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R)) {
					pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_AR;
				}
				else {
					pBVAgrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_AC;
				}
			}
		}
		// Now go ahead and save agreement and fnc doc and payment instruction	
        
		beDocStatHist.doc_stat_cd = pBVAgrmt.agrmt_stat_cd;
		beDocStatHist.doc_stat_hist_id = JClaretyConstants.ZERO_INTEGER;
		beDocStatHist.doc_stat_hist_opt_lck_ctl = JClaretyConstants.ZERO_INTEGER;
		tempdocstatref = dbdocstatref.readByGrpTypNStatCd(JClaretyConstants.DOC_GRP_TYP_CD_OSC,pBVAgrmt.agrmt_stat_cd.trim());
		if (errorsPresent()) {
			return null;
		}
		// Now go ahead and save fnc doc and agreement
		BVFABMGLTransSaveFncDoc bvFABMGLTransSaveFncDoc = null;
		//Not pro-rated & not completed
		bvFABMGLTransSaveFncDoc = fabmgltrans.saveFncDoc(pBEPln, bembr,befncdoc, beDocStatHist,null, pEffDt, pPstngDt);
		if (errorsPresent()) {
			return null;
		}
		pBVAgrmt = dbagrmt.saveAgrmt(pBVAgrmt,pBEOSCRqst,pBVAgrmt.mbr_acct.acct_id);//();//asingh2 method require from DB
		if (errorsPresent()) {
			return null;
		}
		// In this context, let's save the payment instructions, too, in case they have changed
		// TabAgrmt needs to pass this in as TRUE whenever PymtInstr changes
         
		if (pHasPymtInstrChngd == true) {
			int bePymtInstrSize = pBVAgrmt.aobepymtinstr.size();
			for (int j = 0; j < bePymtInstrSize; j++) {
				BEPymtInstr bEPymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(j);
				if (bEPymtInstr.pymt_instr_id == JClaretyConstants.ZERO_INTEGER) {
					beFncItem = null;
				}
				else {
					if (bEPymtInstr.fnc_item_id != null && bEPymtInstr.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
						beFncItem = new BEFncItem();
						beFncItem.fnc_item_id = bEPymtInstr.fnc_item_id.intValue();
					}
					else {
						beFncItem = null;
					}
				}
                
//				82A@C.01
				BEEmprChngDtl pBEEmprChngDtl = new BEEmprChngDtl(); //CCR-77794
				BEEmprChngDtl beEmprChngDtl = new BEEmprChngDtl();
				Date tmpEndDt = null;

				if (bEPymtInstr.opt_lck_ctl != JClaretyConstants.ZERO_INTEGER && bEPymtInstr.pymt_typ != null 
						&& pBVAgrmt != null && pBVAgrmt.curEmprEffFrom != null && pBVAgrmt.forEmprEffFrom != null){
					if(bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR)
							||	bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR)
							||  bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR)
							||  bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL)) { //update
						//M@P.15 Start
//						if((((bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR) 
//								|| bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL)) 
//								&& bEPymtInstr.pyr_typ.trim().equals(JClaretyConstants.OSC_PAYER_TYP_CEMPR))//1A@C.02
//								&& (pBVAgrmt.isCurrEmprChanged != null && pBVAgrmt.isCurrEmprChanged.booleanValue()))
//										||
//								(((bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR)
//								|| bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR))
//								&& bEPymtInstr.pyr_typ.trim().equals(JClaretyConstants.OSC_PAYER_TYP_FEMPR))//1A@C.02
//								&& (pBVAgrmt.isFormEmprChanged != null && pBVAgrmt.isFormEmprChanged.booleanValue()))){
						
//						   M@P.15 End
							beEmprChngDtl = dbEmprChngDtl.readByPymtInstrId(bEPymtInstr.pymt_instr_id);
							if(errorsPresent()){
								return null;
							}
							if(beEmprChngDtl != null){
								pBEEmprChngDtl.empr_chng_dtl_id = beEmprChngDtl.empr_chng_dtl_id;
							}

							BEPymtInstr bepymtinstr = dbPymtInstr.readByPymtInstrId(bEPymtInstr.pymt_instr_id);
							if(errorsPresent()){
								return null;
							}
							if(bepymtinstr != null){
								pBEEmprChngDtl.agrmt_id = pBVAgrmt.agrmt.agrmt_id;
								pBEEmprChngDtl.pymt_instr_id = bepymtinstr.pymt_instr_id;
								pBEEmprChngDtl.eff_dt = bepymtinstr.eff_dt;
								pBEEmprChngDtl.empr_id = bepymtinstr.pyr_id;
								pBEEmprChngDtl.empr_typ = bepymtinstr.pyr_typ;
								pBEEmprChngDtl.pymt_typ = bepymtinstr.pymt_typ;
								pBEEmprChngDtl.empr_name = bepymtinstr.pyr_typ;
							}

							Calendar calStDt = Calendar.getInstance();

							if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR)){
								calStDt.setTime(pBVAgrmt.curEmprEffFrom);
							}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR)){
								calStDt.setTime(pBVAgrmt.forEmprEffFrom);
							}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR)){
								calStDt.setTime(pBVAgrmt.forEmprEffFrom);
							}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL)){
								calStDt.setTime(pBVAgrmt.curEmprEffFrom);
							}

							int pMnthNr = calStDt.get(Calendar.MONTH)+1;
							int pYrNr = calStDt.get(Calendar.YEAR);		
							pMnthNr =  pMnthNr -1;
							tmpEndDt =beDocStatHist.getLastDayOfMnth(pMnthNr, pYrNr);
							if(errorsPresent()){
								return null;
							}

							//pBEEmprChngDtl.valid_upto = bEPymtInstr.eff_dt;
							pBEEmprChngDtl.valid_upto = tmpEndDt;

							pBEEmprChngDtl = dbEmprChngDtl.save(pBEEmprChngDtl);
							if(errorsPresent()){
								return null;
							}//76A@C.04 - Starts 
					}     
                           //M@P.15 Start
//					        else if(bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR) ||
//								bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL)){ 
//							if(pBVAgrmt.curEmprEffFrom != null && 
//									(pBVAgrmt.isEditCurrEmprBtnClicked != null && pBVAgrmt.isEditCurrEmprBtnClicked.booleanValue())){
//
//								List<BEEmprChngDtl> aoBEEmprChgDtl = dbEmprChngDtl.readByAgrmtIdAndPymtInstrId(pBVAgrmt.agrmt.agrmt_id ,bEPymtInstr.pymt_instr_id);
//								if(errorsPresent()){
//									return null;
//								}
//								BEEmprChngDtl bEEmprChngDtl = null;
//								if(aoBEEmprChgDtl != null && aoBEEmprChgDtl.size() > JClaretyConstants.ZERO_INTEGER){
//									bEEmprChngDtl = aoBEEmprChgDtl.get(JClaretyConstants.ZERO_INTEGER);
//								}else{
//									continue;
//								}
//
//								Calendar calStDt = Calendar.getInstance();
//								calStDt.setTime(pBVAgrmt.curEmprEffFrom);
//
//								int pMnthNr = calStDt.get(Calendar.MONTH)+1;
//								int pYrNr = calStDt.get(Calendar.YEAR);		
//								pMnthNr =  pMnthNr -1;
//								tmpEndDt = beDocStatHist.getLastDayOfMnth(pMnthNr, pYrNr);
//								if(errorsPresent()){
//									return null;
//								}
//								
//								if(bEEmprChngDtl != null){
//									bEEmprChngDtl.valid_upto = tmpEndDt;
//
//									pBEEmprChngDtl = dbEmprChngDtl.save(bEEmprChngDtl);
//									if(errorsPresent()){
//										return null;
//									}
//								}
//							}else{
//								continue;
//							}
////						}else if(bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR) || 
//								bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR)){
//							if(pBVAgrmt.forEmprEffFrom != null && 
//									(pBVAgrmt.isEditFrmrEmprBtnClicked != null && pBVAgrmt.isEditFrmrEmprBtnClicked.booleanValue())){
//
//								List<BEEmprChngDtl> aoBEEmprChgDtl = dbEmprChngDtl.readByAgrmtIdAndPymtInstrId(pBVAgrmt.agrmt.agrmt_id ,bEPymtInstr.pymt_instr_id);
//								if(errorsPresent()){
//									return null;
//								}
//								BEEmprChngDtl bEEmprChngDtl = null;
//								if(aoBEEmprChgDtl != null && aoBEEmprChgDtl.size() > JClaretyConstants.ZERO_INTEGER){
//									bEEmprChngDtl = aoBEEmprChgDtl.get(JClaretyConstants.ZERO_INTEGER);
//								}else{
//									continue;
//								}
//
//								Calendar calStDt = Calendar.getInstance();
//								calStDt.setTime(pBVAgrmt.forEmprEffFrom);
//
//								int pMnthNr = calStDt.get(Calendar.MONTH)+1;
//								int pYrNr = calStDt.get(Calendar.YEAR);		
//								pMnthNr =  pMnthNr -1;
//								tmpEndDt = beDocStatHist.getLastDayOfMnth(pMnthNr, pYrNr);
//								if(errorsPresent()){
//									return null;
//								}
//								
//								if(bEEmprChngDtl != null){
//									bEEmprChngDtl.valid_upto = tmpEndDt;
//                                    
//									pBEEmprChngDtl = dbEmprChngDtl.save(bEEmprChngDtl);
//									if(errorsPresent()){
//										return null;
//									}
//								}
//							}else{
//								continue;
//							}
//						}//76A@C.04 - Ends
//					}M@P.15 End
				}
				//pBEEmprChngDtl.opt_lck_ctl = bEPymtInstr.opt_lck_ctl;
				if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR) && pBVAgrmt.curEmprEffFrom != null){
					bEPymtInstr.eff_dt = pBVAgrmt.curEmprEffFrom;
					//pBVAgrmt.agrmt.eff_dt = pBVAgrmt.curEmprEffFrom; //Open this if be_agrmt is also needs to be modified.
				}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR) && pBVAgrmt.forEmprEffFrom != null){
					bEPymtInstr.eff_dt = pBVAgrmt.forEmprEffFrom;
					//pBVAgrmt.agrmt.eff_dt = pBVAgrmt.forEmprEffFrom;
				}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR) && pBVAgrmt.forEmprEffFrom != null){
					bEPymtInstr.eff_dt = pBVAgrmt.forEmprEffFrom;
					//pBVAgrmt.agrmt.eff_dt = pBVAgrmt.forEmprEffFrom;
				}else if(bEPymtInstr.pymt_typ != null && bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL) && pBVAgrmt.curEmprEffFrom != null){
					bEPymtInstr.eff_dt = pBVAgrmt.curEmprEffFrom;
				}/*else if(pBVAgrmt.curEmprEffFrom == null || pBVAgrmt.forEmprEffFrom == null){//1M@C.03
					bEPymtInstr.eff_dt = pBVAgrmt.agrmt.eff_dt;
				}*/
                
				//bEPymtInstr.eff_dt =  pBVAgrmt.agrmt.eff_dt;
				bEPymtInstr=dbPymtInstr.save(pBVAgrmt.agrmt, beFncItem, bEPymtInstr);
				if(errorsPresent()){
					return null;
				}
			}
		}


		///////////////////////////////// Some trailing stuff ////////////////////////////////////////////////
		// If the status of agreement is pending and the agreement is new then attach all the relevant funding 
		// requirements to agreement
		// Else if the agreement is getting completed then post service
		//////////////////////////////////////////////////////////////////////////////////////////////////////
		// Attach funding to agreement
		BEFndgRqmt beFndgRqmt = null;

		beSrvCrdtRef = dbsrvcrdtref.readByCostSchd(pBVAgrmt.cost_schd, null);
		if (errorsPresent()) {
			return null;
		}
		if (beSrvCrdtRef != null) {
			//When an agreement gets saved in the pending status, we are going to attach the agreement to the 
			//purchase funding requirements. This is to handle multiple open agreement with same srv_crdt_typ_cd 			for a
			//member. After linking agreement to funding requirement, tracking service and cost of purchase per	agreement
			// should be real easy. 
			//Before an agreement gets deleted from pending status we will go ahead and delink the funding 	requirements
			//from the agreement
			if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_P) && isNewAgrmt) {
				// For non-refund agreements, tracing funding requirement is easy. Just follow the cost schedule id and 
				// bring in those which are not already linked to some agreement
				if (!beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_RFND)) {
					aoBVFndgRqmtCntrb = dbfndgrqmt.readUnfundedBySchdId(pBVAgrmt.mbr_acct.acct_id,pBVAgrmt.cost_schd.cost_schd_id,
							JClaretyConstants.ZERO_INTEGER);
					if (errorsPresent()) {
						return null;
					}
				}
				else {
					// However for refund buyback cases, tracing funding requirements are going to be a little
					// convoluted. First figure whether a refund is getting bought fully. If yes, then 	link all the
					// buyback fundings to the agreement.
					// If not,i.e., if the buyback is pick and choose then go ahead and read a link tableto retrieve the 
					// picked information and save only as many funding as is needed.
					// In this whole process we might land up prorating funding!!! But we imagine that 					the 
					// probability of such case is really less.
					BVFabmOscPopulateRefundBuybackDetail bvFabmOscPopulateRefundBuybackDetail = null;
					bvFabmOscPopulateRefundBuybackDetail = this.populateRefundBuybackDetail(pBVAgrmt.mbr_acct,bvOSCRqstInfo, null);
					if (errorsPresent()) {
						return null;
					}
					aoBVYrlyRfndTransChosen = bvFabmOscPopulateRefundBuybackDetail.aoBVYrlyRfndTransChosen;
					boolean fullBuyBack = false;
					boolean includeAll = true;
					boolean excludeAll = false;
					int bvOSCRqstInfoSize = bvOSCRqstInfo.aobvrfndtrans.size();
					BVRfndTrans bvRfndTrans = null;
					int size = bvOSCRqstInfoSize;
					List aoSelctedPyrldtl = null;//1A@P.11
					for (int k = 0; k < size; k++) {
						bvRfndTrans = (BVRfndTrans) bvOSCRqstInfo.aobvrfndtrans.get(k);
						if (bvRfndTrans.incl_in == true) {
							if (bvRfndTrans.aoBVYrlyRfndTrans == null || bvRfndTrans.aoBVYrlyRfndTrans.size() == 
								JClaretyConstants.ZERO_INTEGER) {
								fullBuyBack = true;
							}
							else {
								int size2 = bvRfndTrans.aoBVYrlyRfndTrans.size();
								for (int i = 0; i < size2; i++) {
									BVYrlyRfndTrans bVYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(i);
									//4A@P.12
									if (bVYrlyRfndTrans.incl_in == false) {
										includeAll = false;
										break;
									}
									//1M@P.01
									//4M@P.07: condition added to include only those pyrl which were selected during cost calc
									if(bVYrlyRfndTrans.aoDetailList !=null && bVYrlyRfndTrans.aoDetailList.size()>JClaretyConstants.ZERO_INTEGER){
										for(int n=0;n<bVYrlyRfndTrans.aoDetailList.size();n++){
												bVYrlyRfndTrans=(BVYrlyRfndTrans)	bVYrlyRfndTrans.aoDetailList.get(n);
												if (bVYrlyRfndTrans.incl_in == false) {
													includeAll = false;
													break;
												}
										}
									}/*6D@P.12 else{
											if (bVYrlyRfndTrans.incl_in == false) {
												includeAll = false;
												break;
											}
										
									}*/
									if(includeAll==false){
										break;
									}
									//4D@P.12
									/*if (bVYrlyRfndTrans.incl_in == false) {
										includeAll = false;
										break;
									}*/
								}
								int size3 = bvRfndTrans.aoBVYrlyRfndTrans.size();
								for (int j = 0; j < size3; j++) {
									BVYrlyRfndTrans bVYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(j);
									//1M@P.01
									if (bVYrlyRfndTrans.incl_in == true) {
										excludeAll = true;
										break;
									}
								}
								// If all the rows of aoBVYrlyRfndTrans has its incl_in as false then this
								//transaction is not be considered at all. In this case both the boolean variables
								//would be false as per the above lines. Donot set the Fullbuyback indicator and the
								//rest is good to go ie.,(The else part of "fullbuyback = true"
								//would compare the fiscal years and would do nothing with this
								//transaction).
								if (excludeAll == false || includeAll == true) {
									//1M@P.01
									if (excludeAll == false && includeAll == false) {
										//donot set the indicator
										//For MREF, the above two indicators would be false as
										//the detail window would not be opened at all. set the indicator to true
										if (bvOSCRqstInfo != null && bvOSCRqstInfo.cost_calc_typ.trim().equals(JClaretyConstants.SRV_CRDT_CLI_CD_MREF)) {
											fullBuyBack = true;
										}
									}
									else {
										fullBuyBack = true;
									}
								}
							}
							if (fullBuyBack == true) {
								tmpaoBVFndgRqmtCntrb = dbfndgrqmt.readByOSCRqst(pBVAgrmt.mbr_acct.acct_id,bvOSCRqstInfo.osc_rqst_id,
										bvRfndTrans.acct_trans_id);
								if (errorsPresent()) {
									return null;
								}
							}
							else {
								tmpaoBVFndgRqmtCntrb = new ArrayList();
								//1A@P.11
								aoSelctedPyrldtl = new ArrayList();
								if (aoBVYrlyRfndTransChosen != null && aoBVYrlyRfndTransChosen.size() != JClaretyConstants.ZERO_INTEGER) {
									int size4 = aoBVYrlyRfndTransChosen.size();
									for (int i = 0; i < size4; i++) {
										BVYrlyRfndTrans bvYrlyRfndTransChosen = (BVYrlyRfndTrans) aoBVYrlyRfndTransChosen.get(i);
										int size5 = bvRfndTrans.aoBVYrlyRfndTrans.size();
										for (int j = 0; j < size5; j++) {
											BVYrlyRfndTrans bVYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(j);
											//1M@P.11 to save only those fndgs which are included in cost calc tab
											if (bVYrlyRfndTrans.incl_in && bvYrlyRfndTransChosen.fscl_yr == bVYrlyRfndTrans.fscl_yr) {
												for (int m = 0; m < bVYrlyRfndTrans.aobvpyrldtl.size(); m++) {
													BVPyrlDtl bvpyrldtl = (BVPyrlDtl) bVYrlyRfndTrans.aobvpyrldtl.get(m);
													//6A@P.07: condition added to include only those pyrl which were selected during cost calc
													if(bvYrlyRfndTransChosen.aoDetailList!=null && bvYrlyRfndTransChosen.aoDetailList.size()>JClaretyConstants.ZERO_INTEGER &&
													((BVYrlyRfndTrans)bvYrlyRfndTransChosen.aoDetailList.get(m)).incl_in==true){
														tempFndgRqmtCntrb = new BVFndgRqmtCntrb();
														tempFndgRqmtCntrb.setParentAttributes(bvpyrldtl.befndgrqmt);
														tmpaoBVFndgRqmtCntrb.add(tempFndgRqmtCntrb);
														aoSelctedPyrldtl.add(bvpyrldtl);
													}else if(bvYrlyRfndTransChosen.aoDetailList==null || 
													bvYrlyRfndTransChosen.aoDetailList.size()==JClaretyConstants.ZERO_INTEGER){
														tempFndgRqmtCntrb = new BVFndgRqmtCntrb();
														tempFndgRqmtCntrb.setParentAttributes(bvpyrldtl.befndgrqmt);
														tmpaoBVFndgRqmtCntrb.add(tempFndgRqmtCntrb);
														aoSelctedPyrldtl.add(bvpyrldtl);
													}
													
												}
												break;
											}
										}
									}
								}
							}
							if (tmpaoBVFndgRqmtCntrb != null && tmpaoBVFndgRqmtCntrb.size() != JClaretyConstants.ZERO_INTEGER) {
								int size6 = tmpaoBVFndgRqmtCntrb.size();
								for (int i = 0; i < size6; i++) {
									BVFndgRqmtCntrb tempBVFndgRqmtCntrb = (BVFndgRqmtCntrb) tmpaoBVFndgRqmtCntrb.get(i);
									aoBVFndgRqmtCntrb.add(tempBVFndgRqmtCntrb);
								}
							}
							
							//to make the cntrb stat in service hist as pending while saving the agreement first time
							//P.11 starts
							if(aoSelctedPyrldtl!=null){
								List aoTPSrvStatHist = new ArrayList();
								for (int i = 0; i < aoSelctedPyrldtl.size(); i++) {
									BVPyrlDtl bvPyrlDtl = (BVPyrlDtl) aoSelctedPyrldtl.get(i);
									if(bvPyrlDtl.becntrb.stat_cd != null && bvPyrlDtl.becntrb.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_RFND)){
										TpSrvStatHist tpSrvStatHist = new TpSrvStatHist();
										tpSrvStatHist.opt_lck_ctl = JClaretyConstants.ONE_INTEGER;
										tpSrvStatHist.rcd_crt_nm = pBVAgrmt.agrmt.rcd_crt_nm;
										tpSrvStatHist.acct_id = pBVAgrmt.mbr_acct.acct_id;
										tpSrvStatHist.cntrb_id = bvPyrlDtl.becntrb.cntrb_id;
										tpSrvStatHist.cntrb_stat_cd = JClaretyConstants.CNTRB_STAT_CD_PNDG;
										tpSrvStatHist.stat_chg_dt = getTimeMgr().getCurrentDateOnly();
										aoTPSrvStatHist.add(tpSrvStatHist);
									}
								}
								dbcntrb.updateCntrbStatArray(aoTPSrvStatHist);
								if (errorsPresent()) {
									
								}	
							}//P.11 ends
						
						}//
					}
				}
				//If aoBVFndgRqmtCntrb looks like it does not have any element then there is nothing available
				//for the agreement. In that case come out of the save with message
				if (aoBVFndgRqmtCntrb == null || aoBVFndgRqmtCntrb.size() == JClaretyConstants.ZERO_INTEGER) {
					addErrorMsg(1037,new String[]{"Cannot find service information that this agreement can buy"});
					return null;
				}
				BVFabmMbrAcctMaintCalcContrb bvFabmMbrAcctMaintCalcContrb = null;
				if (beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)) {
					BEPyrl bePyrl = null;
					BEEmpr beEmpr = null;
					BigDecimal eECntrbAmt = JClaretyConstants.ZERO_BIGDECIMAL;
					BigDecimal eRCntrbAmt = JClaretyConstants.ZERO_BIGDECIMAL;
					boolean mkupEROverridden = false;
					if (pBEPln.pln_id != JClaretyConstants.PLN_ID_JUDGES && pBEPln.pln_id != JClaretyConstants.PLN_ID_JDGS_7) {
						int size = aoBVFndgRqmtCntrb.size();
						for (int i = 0; i < size; i++) {
							BVFndgRqmtCntrb tmpbvFndgRqmtCntrb = (BVFndgRqmtCntrb) aoBVFndgRqmtCntrb.get(i);
							bePyrl = dbpyrl.readByPyrlId(tmpbvFndgRqmtCntrb.becntrb.pyrl_id);
							if (errorsPresent()) {
								return null;
							}
							beEmpr = dbempr.readByOrgId(pBVAgrmt.cost_schd.org_id);
							if (errorsPresent()) {
								return null;
							}
							if (beEmpr != null) {
								bvFabmMbrAcctMaintCalcContrb = fabmmbracctmaint.calcContribution(pBVAgrmt.mbr_acct,bePyrl,beEmpr,null);
								if (errorsPresent()) {
									return null;
								}
								eECntrbAmt = bvFabmMbrAcctMaintCalcContrb.EECntrbAmt;
								eRCntrbAmt = bvFabmMbrAcctMaintCalcContrb.ERCntrbAmt;
								if (tmpbvFndgRqmtCntrb.er_share.compareTo(eRCntrbAmt) != JClaretyConstants.ZERO_INTEGER) {
									mkupEROverridden = true;
									pBVAgrmt.agrmt.mkup_er_override_in = true;
									pBVAgrmt=dbagrmt.saveAgrmt(pBVAgrmt,pBEOSCRqst,pBVAgrmt.mbr_acct.acct_id);									
									if (errorsPresent()) {
										return null;
									}
									break;
								}
							}
						}
					}
				}
				BigDecimal sumTotRqdAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal sumERShare = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal changedTotRqdAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal changedERShare = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal diffTotRqdAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal diffERShare = JClaretyConstants.ZERO_BIGDECIMAL;
				int size = aoBVFndgRqmtCntrb.size();
				for (int j = 0; j < size; j++) {
					BVFndgRqmtCntrb bvfndgRqmtCntrb = (BVFndgRqmtCntrb) aoBVFndgRqmtCntrb.get(j);
					bvfndgRqmtCntrb.agrmt_id = pBVAgrmt.agrmt.agrmt_id;
					beFndgRqmt = bvfndgRqmtCntrb;
					beFndgRqmt = dbfndgrqmt.save(beFndgRqmt,JClaretyConstants.ZERO_INTEGER);
					if (errorsPresent()) {
						return null;
					}//2A@P.03
					sumTotRqdAmt = sumTotRqdAmt.add(beFndgRqmt.tot_rqd_amt);
					sumERShare = sumERShare.add(beFndgRqmt.er_share);
				}//30A@P.03
				if(pBVAgrmt.agrmt.srv_crdt_typ.trim().equalsIgnoreCase("MKUP")){
				if(pBVAgrmt != null && pBVAgrmt.aobepymtinstr != null){
					for(int t=0;t< pBVAgrmt.aobepymtinstr.size();t++){
						BEPymtInstr  bepyInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(t);
						if(bepyInstr.pymt_typ.trim().equalsIgnoreCase("MPYR")){
							changedTotRqdAmt = bepyInstr.orig_amt;
						}
						if(bepyInstr.pymt_typ.trim().equalsIgnoreCase("EPYR")){
							changedERShare = bepyInstr.orig_amt;
						}
					}
				}
				diffTotRqdAmt = changedTotRqdAmt.subtract(sumTotRqdAmt);
				diffERShare = changedERShare.subtract(sumERShare);
				if(aoBVFndgRqmtCntrb != null){
				for (int j =  aoBVFndgRqmtCntrb.size()-1; j >= 0; j--) {
					BVFndgRqmtCntrb bvfndgRqmtCntrb = (BVFndgRqmtCntrb) aoBVFndgRqmtCntrb.get(j);
					if(bvfndgRqmtCntrb.tot_rqd_amt.compareTo(diffTotRqdAmt) >= JClaretyConstants.ZERO_INTEGER  //2M@P.04
							&& bvfndgRqmtCntrb.er_share.compareTo(diffERShare) >= JClaretyConstants.ZERO_INTEGER){
						bvfndgRqmtCntrb.tot_rqd_amt = bvfndgRqmtCntrb.tot_rqd_amt.add(diffTotRqdAmt);
						bvfndgRqmtCntrb.er_share = bvfndgRqmtCntrb.er_share.add(diffERShare);
						beFndgRqmt = bvfndgRqmtCntrb;
						beFndgRqmt = dbfndgrqmt.save(beFndgRqmt,JClaretyConstants.ZERO_INTEGER);
						if (errorsPresent()) {
							return null;
						}
						break;
					}
				}
				}
			}
			}
			else if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_V)) {
				// If an agreement is voided then go ahead and delink the fundings from the agreement
				List aoBVPyrlDtl;
				aoBVPyrlDtl = dbfndgrqmt.readBVPyrlDtlByAgrmt(pBVAgrmt.agrmt,pBVAgrmt.mbr_acct.acct_id);
				if (errorsPresent()) {
					return null;
				}
				if (aoBVPyrlDtl != null && aoBVPyrlDtl.size() != JClaretyConstants.ZERO_INTEGER) {
					int size = aoBVPyrlDtl.size();
					for (int j = 0; j < size; j++) {
						BVPyrlDtl bvpyrlDtl = (BVPyrlDtl) aoBVPyrlDtl.get(j);
						bvpyrlDtl.befndgrqmt.agrmt_id = JClaretyConstants.ZERO_INTEGER;
						beFndgRqmt = bvpyrlDtl.befndgrqmt;
						beFndgRqmt = dbfndgrqmt.save(beFndgRqmt,JClaretyConstants.ZERO_INTEGER);
						if (errorsPresent()) {
							return null;
						}
					}
				}
			}
		}
		else {
			addErrorMsg(155,new String[]{"service type reference"});
		}


		//Pro-rated/Completed/Maintain-completed
		if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R)||// Prorated
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C)||// Completed
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC)||// Prorated or Completed from PMA
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MR)||// Prorated or Completed from PMA
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_T)) {// Transferred
			if (beplnRef.pln_grp_cd.trim().equals(JClaretyConstants.PLN_GRP_CD_DB)) {
				bvFabmOscSaveAgrmt.pPstdSrvCrdtAmt=this.postService(pBEPln, pBVAgrmt.mbr_acct, pBVAgrmt, pEffDt, pPstngDt, pHideTransIn, pTabMaintain, 0);
				
				
				// A@P.16 --Start
				// Requirement #9, For school plan, when posting a OOS/Airtime checking if it is in lower cost tier
				if ((pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL) && (pBEOSCRqst != null)
						&& (pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_OOS)
								|| pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT))) {
					boolean compareTierFactors = false;

					compareTierFactors = this.compareCostTierFactors(pBEOSCRqst, pBVAgrmt.mbr_acct);

					if (!compareTierFactors) {
						// Member is in higher cost tier but trying to purchase with the
						// lower cost tier
						addErrorMsg(0, new String[] {
								" Cannot purchase OOS/Airtime with lower cost tier as the member is in higher cost tier " });
						return null;
					}
				}

				// Requirement #10, Error during posting the Rfnd or MKUP if OOS or airtime was purchased
				//Raise an error when posting a service credit thru any process except PMA
				if(!(pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC)|| 
						pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MR))){
				if ((pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL) && (pBEOSCRqst != null)
						&& (pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)
								|| pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP))) {

					BigDecimal srvQty = JClaretyConstants.ZERO_BIGDECIMAL;
					List<BVSrvcCrdt> aoBVSrvcCrdt = fabmmbracctmaint.readSrvcCrdtForTotByTypeNStat(pBVAgrmt.mbr_acct,
							false, -1, -1, null, bepln, null, false, false);
					if (errorsPresent()) {
						return null;
					}

					if (aoBVSrvcCrdt != null) {
						for (BVSrvcCrdt row : aoBVSrvcCrdt) {
							if (row.srv_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_OOS)
									|| (row.srv_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT))) {
								srvQty = srvQty.add(row.srv_eqvt_qty);
							}
						}
					}
					if (srvQty.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
						List<BEOSCRqst> beOSCRqstList = dboscrqst.readByMbrAcct(pBVAgrmt.mbr_acct);
					if (errorsPresent()) {
						return null;
					}
						for (BEOSCRqst beOSCRqst : beOSCRqstList) {
							if (beOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_OOS)
									|| (beOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT))
											&& (beOSCRqst.cube_factor.compareTo(
													pBEOSCRqst.cube_factor) < JClaretyConstants.ZERO_INTEGER)) {
								addErrorMsg(0, new String[] {
										" An OOS or Airtime was already purchased with lower cost tier factors" });
								return null;
							}
						}
					}
				}
			  }// A@P.16 --End

			}
			// When an air time agreement gets completed, we need to change the status of the service credit to HELD, 
			// untill refund/ retirement takes place
		}
		else if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_AR)||// Air-time Prorated
				pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_AC)) {// Air-time completed
			List aoBVPyrlDtl;
			//NE.01 - Beging.
			List aoTPSrvStatHist = new ArrayList();
			//NE.01 - End.
			aoBVPyrlDtl = dbfndgrqmt.readBVPyrlDtlByAgrmt(pBVAgrmt.agrmt,pBVAgrmt.mbr_acct.acct_id);
			if (errorsPresent()) {
				return null;
			}
			if (aoBVPyrlDtl != null) {
				int size = aoBVPyrlDtl.size();
				for (int i = 0; i < size; i++) {
					//BVPyrlDtl bvPyrlDtl = new BVPyrlDtl();
					TpSrvStatHist tmpTPSrvStatHist = new TpSrvStatHist();
					tmpTPSrvStatHist.acct_id = pBVAgrmt.mbr_acct.acct_id;
					tmpTPSrvStatHist.cntrb_id = ((BVPyrlDtl) aoBVPyrlDtl.get(i)).becntrb.cntrb_id;
					tmpTPSrvStatHist.cntrb_stat_cd = JClaretyConstants.CNTRB_STAT_CD_HELD;
					tmpTPSrvStatHist.stat_chg_dt = pPstngDt;
					aoTPSrvStatHist.add(tmpTPSrvStatHist);
				}
				dbcntrb.updateCntrbStatArray(aoTPSrvStatHist);
				if (errorsPresent()) {
					return null;
				}
			}
		}

		if ( tempdocstatref != null) {
			pBVAgrmt.agrmt_status = tempdocstatref.doc_stat_cd;
		}
		BVAgrmt bvAgrmt = null;

		bvAgrmt = this.getBVAgrmtSmry(pBEPln, pBVAgrmt);
		if (errorsPresent()) {
			return null;
		}
		BVAgrmtARLink bvAgrmtARLink = null;//1AP.05
		//P.02 starts
		//M@P.14 - Reformat start 
		if (bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R)
				|| bvAgrmt.agrmt_stat_cd.trim().equals(
						JClaretyConstants.AGRMT_STAT_C)) {

			pBVAgrmt = dbagrmt.readByAgrmtId(pBVAgrmt.agrmt.agrmt_id);
			if (beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(
					JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)) {// 1A@P.06
				if (pBVAgrmt != null
						&& pBVAgrmt.beAcctRcvbl != null
						&& pBVAgrmt.beAcctRcvbl.acct_rcvbl_id == JClaretyConstants.ZERO_INTEGER) { // 1M@P.14
					// && createRcvbl){//1A@P.08 //1D@P.14
					bvAgrmtARLink = this.createRcvblsForEmpr(pBEPln, bvAgrmt,
							pEffDt, pPstngDt, beOrg);// 8A@P.05
					if (bvAgrmtARLink != null
							&& bvAgrmtARLink.rcvbl_amt
									.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
						if (bePymtInstr.pre_tax_in) {
							pBVAgrmt.agrmt.paid_pre_tax_amt = pBVAgrmt.agrmt.paid_pre_tax_amt
									.add(bvAgrmtARLink.rcvbl_amt);
						} else {
							pBVAgrmt.agrmt.paid_post_tax_amt = pBVAgrmt.agrmt.paid_post_tax_amt
									.add(bvAgrmtARLink.rcvbl_amt);
						}
						pBVAgrmt = dbagrmt.saveAgrmt(pBVAgrmt, pBEOSCRqst,
								pBVAgrmt.mbr_acct.acct_id);
					}
				}
			}
		}
		//M@P.14 - Reformat end
		//P.02 ends

		
		bvFabmOscSaveAgrmt.pBVAgrmt = bvAgrmt;
		addTraceMessage("saveAgrmt", FABM_METHODEXIT);
		takeBenchMark("saveAgrmt", FABM_METHODEXIT);
		return bvFabmOscSaveAgrmt;
	}
	/*********************************************************
	 * Method Name : populateRefundBuybackDetail()
	 *
	 * Input Parameters	:
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *	P_BVOSCRqstInfo : BV_OSCRqstInfo
	 *	P_IncludeAgrmt : BE_Agrmt
	 *	
	 * Output Parameters	:
	 *	P_BVOSCRqstInfo : BV_OSCRqstInfo
	 *	P_aoBVYrlyRfndTransChosen : Array of BV_YrlyRfndTrans
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	If this method is called from Window Cost detail the following attribute must be filled be_osc_rqst and 
	 *	aoBVRfndTrans. If this method is called from other places be_osc_rqst must be filled 
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.0                     asingh2     04/02/2008  Initial conversion from Forte to java
	 * NE.1                     jmisra      04/02/2008  Code modified after review.
	 * P.01	NPRIS-1194			gpannu		12/09/2009	 code added for CCR 53 changes
	 * P.02	NPRIS-6399		    VenkataM	06/28/2017	Added code to avoid adding duplicate rows		
	 * ***********************************************************************************/
	public BVFabmOscPopulateRefundBuybackDetail populateRefundBuybackDetail(BEMbrAcct pBEMbrAcct,BVOSCRqstInfo pBVOSCRqstInfo,
			BEAgrmt pIncludeAgrmt) throws ClaretyException {

		addTraceMessage("populateRefundBuybackDetail",FABM_METHODENTRY);
		takeBenchMark("populateRefundBuybackDetail",FABM_METHODENTRY);

		BVFabmOscPopulateRefundBuybackDetail bvFabmOscPopulateRefundBuybackDetail = new BVFabmOscPopulateRefundBuybackDetail();
		//NE.01 - Begin
		List aoBVPyrlDtl = null;
		//NE.01 - End
		BEAcctTrans bEAcctTrans = new BEAcctTrans();
		List tmpaoBVPyrlDtl = new ArrayList();
		BigDecimal cntrb = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal srv_qty = JClaretyConstants.ZERO_BIGDECIMAL;
		srv_qty.setScale(4);
		int tmpAgrmtid = JClaretyConstants.ZERO_INTEGER;
		//NE.01 - Begin
		Date tmpStrtDt = null;
		Date tmpEndDt = null;
		if(pBVOSCRqstInfo != null){
			if (pBVOSCRqstInfo.aobvrfndtrans == null || pBVOSCRqstInfo.aobvrfndtrans.size() == JClaretyConstants.ZERO_INTEGER) {
				pBVOSCRqstInfo.aobvrfndtrans = dbaccttrans.readRfndTransByOSCRqst(pBVOSCRqstInfo);
				if (errorsPresent()) {
					return null;
				}
			}
		}
		//NE.01 - End
		// At first, first out fiscal yearly information of the refund. This is right from the horse's mouth!!!! I mean
		// right from the member account
		if (pIncludeAgrmt == null) {
			tmpAgrmtid = JClaretyConstants.ZERO_INTEGER;
		}else {
			tmpAgrmtid = pIncludeAgrmt.agrmt_id;
		}

		if (pBVOSCRqstInfo.aobvrfndtrans != null) {
			int size = pBVOSCRqstInfo.aobvrfndtrans.size();
			for (int j = 0; j < size; j++) {
				BVRfndTrans bvRfndTrans = (BVRfndTrans) pBVOSCRqstInfo.aobvrfndtrans.get(j);
				bEAcctTrans.acct_trans_id = bvRfndTrans.acct_trans_id;
				aoBVPyrlDtl = fabmmbracctmaint.readRfndAcctBalByFscYr(pBEMbrAcct,bEAcctTrans, tmpAgrmtid);
				if (errorsPresent()) {
					return null;
				}
				int counter = JClaretyConstants.ZERO_INTEGER;
				int i = JClaretyConstants.ZERO_INTEGER;
				boolean done = false;
				int curStrtYr = JClaretyConstants.INDICATOR_NEG_ONE;
				int curEndYr = JClaretyConstants.INDICATOR_NEG_ONE;
				while (done == false && aoBVPyrlDtl != null) {
					// build array of pyrl detail by fiscal year.
					if (counter < aoBVPyrlDtl.size()
							&& ((BVPyrlDtl)aoBVPyrlDtl.get(counter)).becntrb.fscl_yr_strt_nr == curStrtYr
							&& ((BVPyrlDtl)aoBVPyrlDtl.get(counter)).becntrb.fscl_yr_end_nr == curEndYr) {
						tmpaoBVPyrlDtl.add(((BVPyrlDtl)aoBVPyrlDtl.get(counter)));
						counter = counter + 1;
					}else {
						// If counter has gone past end of array then
						// this is the last time through the loop.  Add in the last fiscal year to the totals in this
						// iteration and then Done boolean will stop while loop.
						if (counter >= aoBVPyrlDtl.size()) {
							done = true;
						}else {
							// counter is not incremented in this section, so that next iteration of while loop	will
							// continue with the row of aoBVSrvQtyStat which triggered this by being in a different year
							curStrtYr = ((BVPyrlDtl)aoBVPyrlDtl.get(counter)).becntrb.fscl_yr_strt_nr;
							curEndYr = ((BVPyrlDtl)aoBVPyrlDtl.get(counter)).becntrb.fscl_yr_end_nr;
							// if this is the first time through there is no point in continuing.
							if(tmpaoBVPyrlDtl.size() == JClaretyConstants.ZERO_INTEGER) {
								continue;
							}
						}// end inner else
						//i = i+1;
						BVYrlyRfndTrans tempBVYrlyRfndTrans = new BVYrlyRfndTrans();
						tempBVYrlyRfndTrans.aobvpyrldtl = new ArrayList();
						for(int z=0; z<tmpaoBVPyrlDtl.size(); z++){
							BVPyrlDtl row = (BVPyrlDtl)tmpaoBVPyrlDtl.get(z);
							tempBVYrlyRfndTrans.aobvpyrldtl.add(row.clone());
						}
						if(bvRfndTrans.aoBVYrlyRfndTrans != null ){
							if( bvRfndTrans.aoBVYrlyRfndTrans.size() > JClaretyConstants.ZERO_INTEGER){
								if(i>=bvRfndTrans.aoBVYrlyRfndTrans.size()){
									bvRfndTrans.aoBVYrlyRfndTrans.add( new ArrayList());
									bvRfndTrans.aoBVYrlyRfndTrans.set(i, tempBVYrlyRfndTrans);								
								}else {
									bvRfndTrans.aoBVYrlyRfndTrans.set(i, tempBVYrlyRfndTrans);
								}
							}else if (bvRfndTrans.aoBVYrlyRfndTrans.size() == JClaretyConstants.ZERO_INTEGER){
								bvRfndTrans.aoBVYrlyRfndTrans.add(0,tempBVYrlyRfndTrans);
							}
						}

						tmpStrtDt = new Date();
						tmpEndDt = new Date();
						tmpStrtDt = ((BVPyrlDtl) tmpaoBVPyrlDtl.get(0)).becntrb.strt_dt;
						tmpEndDt = ((BVPyrlDtl) tmpaoBVPyrlDtl.get(0)).becntrb.end_dt;
						BVPyrlDtl bvPyrlDtl = null;
						if(tmpaoBVPyrlDtl!= null){
							for (int k = 0; k < tmpaoBVPyrlDtl.size(); k++) {
								bvPyrlDtl = (BVPyrlDtl) tmpaoBVPyrlDtl.get(k);
								cntrb = cntrb.add(bvPyrlDtl.befndgrqmt.tot_rqd_amt);
								srv_qty = srv_qty.add(bvPyrlDtl.becntrb.srv_eqvt_qty);
								if(getDateCompare().isDayBefore(bvPyrlDtl.becntrb.strt_dt,tmpStrtDt)){
									tmpStrtDt = bvPyrlDtl.becntrb.strt_dt;
								}
								if(getDateCompare().isDayAfter(bvPyrlDtl.becntrb.end_dt,tmpEndDt)){
									tmpEndDt = bvPyrlDtl.becntrb.end_dt;
								}
							}// end inner for
						}
						BVYrlyRfndTrans bvYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(i);
						bvYrlyRfndTrans.calc_srv_crdt_qty = srv_qty;
						bvYrlyRfndTrans.fscl_yr = ((BVPyrlDtl)tmpaoBVPyrlDtl.get(0)).becntrb.fscl_yr_end_nr;
						bvYrlyRfndTrans.incl_in = false;
						bvYrlyRfndTrans.mbr_cntrb = cntrb;
						bvYrlyRfndTrans.rfnd_acct_trans_id = bvRfndTrans.acct_trans_id;
						bvYrlyRfndTrans.srv_crdt_qty = srv_qty;
						bvYrlyRfndTrans.tot_cost = JClaretyConstants.ZERO_BIGDECIMAL;
						bvYrlyRfndTrans.tot_int = JClaretyConstants.ZERO_BIGDECIMAL;
						bvYrlyRfndTrans.strt_dt = tmpStrtDt;
						bvYrlyRfndTrans.end_dt = tmpEndDt;
						tmpaoBVPyrlDtl.clear();
						cntrb = JClaretyConstants.ZERO_BIGDECIMAL;
						srv_qty = JClaretyConstants.ZERO_BIGDECIMAL;

						i = i + JClaretyConstants.ONE_INTEGER;
					}
				}
			}//end for
		}
		//If we have any information related to pick and choose and override, then retrieve that from the database
		List aoBVYrlyRfndTransChosen = null;
		if (pBVOSCRqstInfo.osc_rqst_id != JClaretyConstants.ZERO_INTEGER) {
			aoBVYrlyRfndTransChosen = dboscrqst.readBVYrlyRfndTransByOSCRqst(pBVOSCRqstInfo);
			if (errorsPresent()) {
				return null;
			}
		}
		// At the end try to match raw info to the user ridden info and set incl_in = TRUE and map overridden data to 
		// the fiscall yearly array
		if (aoBVYrlyRfndTransChosen != null && pBVOSCRqstInfo.aobvrfndtrans!= null) {
			int size1 = pBVOSCRqstInfo.aobvrfndtrans.size();
			for (int i = 0; i < size1; i++) {
				BVRfndTrans bvRfndTrans = (BVRfndTrans) pBVOSCRqstInfo.aobvrfndtrans.get(i);
				if (bvRfndTrans.aoBVYrlyRfndTrans != null){	
					//bvRfndTrans.aoBVYrlyRfndTrans = aoBVYrlyRfndTransChosen;
					int size2 = bvRfndTrans.aoBVYrlyRfndTrans.size();
					for (int j = 0; j < size2; j++) {
						BVYrlyRfndTrans bvYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(j);

						int size3 = aoBVYrlyRfndTransChosen.size();
						for (int k = 0; k < size3; k++) {
							BVYrlyRfndTrans bvYrlyRfndTransChosen = (BVYrlyRfndTrans) aoBVYrlyRfndTransChosen.get(k);
							//M@P.02 
							//Same rows(bvYrlyRfndTransChosen) are being added multiple times and total is calculates wrong when there are two rfnds in the same fscl year
							//So added condition to check rfnd_acct_trans_id which is unique for different rfnd_trans
							if (bvYrlyRfndTrans.fscl_yr == bvYrlyRfndTransChosen.fscl_yr && bvYrlyRfndTrans.rfnd_acct_trans_id == bvYrlyRfndTransChosen.rfnd_acct_trans_id) {
								//1M@P.01
								bvYrlyRfndTrans.incl_in = bvYrlyRfndTransChosen.incl_in;
								bvYrlyRfndTrans.srv_crdt_qty = bvYrlyRfndTransChosen.srv_crdt_qty;
								bvYrlyRfndTrans.mbr_cntrb = bvYrlyRfndTransChosen.mbr_cntrb;
								bvYrlyRfndTrans.tot_cost = bvYrlyRfndTransChosen.tot_cost;
								bvYrlyRfndTrans.tot_int = bvYrlyRfndTransChosen.tot_int;
								bvYrlyRfndTrans.run_tot = bvYrlyRfndTransChosen.run_tot;
								//4M@P.01
								bvYrlyRfndTrans.rfnd_amt = bvYrlyRfndTransChosen.rfnd_amt;
									bvYrlyRfndTrans.rfnd_int = bvYrlyRfndTransChosen.rfnd_int;
								bvYrlyRfndTrans.int_due = bvYrlyRfndTransChosen.int_due;
								bvYrlyRfndTrans.aoDetailList = bvYrlyRfndTransChosen.aoDetailList;
								
							}
					   }
					}
				}
			}
		}
		bvFabmOscPopulateRefundBuybackDetail.aoBVYrlyRfndTransChosen = aoBVYrlyRfndTransChosen;
		bvFabmOscPopulateRefundBuybackDetail.bvOSCRqstInfo = pBVOSCRqstInfo;
		addTraceMessage("populateRefundBuybackDetail",FABM_METHODEXIT);
		takeBenchMark("populateRefundBuybackDetail",FABM_METHODEXIT);
		return bvFabmOscPopulateRefundBuybackDetail;
	}
	/*
	 * 
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.0                    psalwan    04/1/2008  Initial conversion from Forte to java 
	 *                                                  
	 *************************************************************************************/
	//GetFscYrlyAllocInfoRem(P_BVAgrmt:BV_Agrmt, P_BEPln:BE_Pln, P_FY:integer, output P_FYAmt:DecimalData) : ErrorStack
	public BigDecimal getFscYrlyAllocInfo(BVAgrmt bvAgrmt,BEPln bePln, int fY) throws ClaretyException {
		startBenchMark("getFscyrlyAllocInfo",FABM_METHODENTRY);
		addTraceMessage("getFscyrlyAllocInfo",FABM_METHODENTRY);
		BigDecimal fYAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoBEFncAlloc = new ArrayList();
		int fscYr;
		//GetBVAgrmtSmryP_BEPln:BE_Pln, input output P_BVAgrmt:BV_Agrmt)
		bvAgrmt = this.getBVAgrmtSmry(bePln, bvAgrmt);
		if(errorsPresent()){
			return null;
		}
		if (bvAgrmt.aobvagrmtsmry != null) {// bvAgrmt.aobvagrmtsmry = nill
			int size = bvAgrmt.aobvagrmtsmry.size();
			for (int cntr = 0; cntr < size; cntr++) {// for loop 1
				BVAgrmtSmry bvAgrmtSmry = (BVAgrmtSmry) bvAgrmt.aobvagrmtsmry.get(cntr);
				if (bvAgrmtSmry.bvpymtinstr.bepymtinstr.pre_tax_in == false) {// if 1
					if (bvAgrmtSmry.bvpymtinstr.befncitem != null) {// if 2
						//Read aoBEFncAlloc by BEFncItem
						aoBEFncAlloc = dbfncalloc.readByToItem(bvAgrmtSmry.bvpymtinstr.befncitem);
						if (aoBEFncAlloc != null) {//aoBEFncAlloc null
							int size1 = aoBEFncAlloc.size();
							for (int i = 0; i < size1; i++) {
								BEFncAlloc beFncAlloc = (BEFncAlloc) aoBEFncAlloc.get(i);
								//Find out fiscal year of the Allocation date
								fscYr = dbfscyr.calculateFiscalYearNr(bePln,beFncAlloc.alloc_dt);
								if(errorsPresent()){
									return null;
								}
								//check if alloc happened in P_FY then
								if (fscYr == fY) {
									//add alloc_amt to FYAmt
									fYAmt = fYAmt.add(beFncAlloc.alloc_amt);
								}
							}
						}//aoBEFncAlloc null ends
					}//if 2 ends
				}// if 1 ends
			}//for loop 1 ends
		}// bvAgrmt.aobvagrmtsmry = nill
		addTraceMessage("getFscyrlyAllocInfo",FABM_METHODEXIT);
		takeBenchMark("getFscyrlyAllocInfo",FABM_METHODEXIT);
		return fYAmt;
	}
	
	/*
	 * 
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * P.01		NPRIS-5570		MKolm		10/14/2015	Initial coding 
	 * P.02		NPRIS-7141		VenkataM	07/12/2021	include last_pymt_dt to calculate fiscal year amount 
	 *************************************************************************************/
	public BigDecimal getYearlyAllocInfo(BVAgrmt bvAgrmt,BEPln bePln, int pEffectiveYear)throws ClaretyException {
		startBenchMark("getYearlyAllocInfo",FABM_METHODENTRY);
		addTraceMessage("getYearlyAllocInfo",FABM_METHODENTRY);
		BigDecimal fYAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoBEFncAlloc = new ArrayList();
		int fscYr;
		
		//GetBVAgrmtSmryP_BEPln:BE_Pln, input output P_BVAgrmt:BV_Agrmt)
		bvAgrmt = this.getBVAgrmtSmry(bePln, bvAgrmt);
		if(errorsPresent()){
			return null;
		}
		if (bvAgrmt.aobvagrmtsmry != null) {// bvAgrmt.aobvagrmtsmry = null
			int size = bvAgrmt.aobvagrmtsmry.size();
			for (int cntr = 0; cntr < size; cntr++) {// for loop 1
				BVAgrmtSmry bvAgrmtSmry = (BVAgrmtSmry) bvAgrmt.aobvagrmtsmry.get(cntr);
				if (bvAgrmtSmry.bvpymtinstr.bepymtinstr.pre_tax_in == false) {// if 1
					if (bvAgrmtSmry.bvpymtinstr.befncitem != null) {// if 2
						//Read aoBEFncAlloc by BEFncItem
						aoBEFncAlloc = dbfncalloc.readByToItem(bvAgrmtSmry.bvpymtinstr.befncitem);
						if (aoBEFncAlloc != null) {//aoBEFncAlloc null
							int size1 = aoBEFncAlloc.size();
							for (int i = 0; i < size1; i++) {
								BEFncAlloc beFncAlloc = (BEFncAlloc) aoBEFncAlloc.get(i);
								
								Calendar cal = Calendar.getInstance();

								// payment dt and alloc dt can be in different years. In that case consider the year of payment date
								if(bvAgrmtSmry.bvpymtinstr.bepymtinstr.last_pymt_dt.before(beFncAlloc.alloc_dt)) {
									
									cal.setTime(bvAgrmtSmry.bvpymtinstr.bepymtinstr.last_pymt_dt);
								} else {
								
									cal.setTime(beFncAlloc.alloc_dt);
								}
								
								if (pEffectiveYear == cal.get(Calendar.YEAR)){
									//add alloc_amt to FYAmt
									fYAmt = fYAmt.add(beFncAlloc.alloc_amt);
								}
							}
						}//aoBEFncAlloc null ends
					}//if 2 ends
				}// if 1 ends
			}//for loop 1 ends
		}// bvAgrmt.aobvagrmtsmry = nill
		addTraceMessage("getYearlyAllocInfo",FABM_METHODEXIT);
		takeBenchMark("getYearlyAllocInfo",FABM_METHODEXIT);
		return fYAmt;
	}
	
	/*  Purpose:
			GetCostSchdSmry method has been deleted for NE.
	 *	It has been modified to summarize an agreement payment upto the passed date 
	 *	(original amount, principal amount, and paid to date)
	 *	Make Up and Purchase Military the agreement is broken down by the payer types
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.0                     asingh2     04/02/2008  Initial conversion from Forte to java
	 * ***********************************************************************************/
	public BVAgrmt getBVAgrmtSmry(BEPln pBEPln,BVAgrmt pBVAgrmt) throws ClaretyException {
		addTraceMessage("getBVAgrmtSmry", FABM_METHODENTRY);
		takeBenchMark("getBVAgrmtSmry", FABM_METHODENTRY);
		BVAgrmtSmry tmpBVAgrmtSmry = null;
		BEFncItem tmpBEFncItem = new BEFncItem();
		BigDecimal item_amt = JClaretyConstants.ZERO_BIGDECIMAL;
		BESrvCrdtRef bESrvCrdtRef = new BESrvCrdtRef();
		String tempSrvCrdttype = pBVAgrmt.agrmt.srv_crdt_typ.trim();
		if (tempSrvCrdttype.trim().equals(JClaretyConstants.SRV_CRDT_CLI_CD_VREF)||
				tempSrvCrdttype.trim().equals(JClaretyConstants.SRV_CRDT_CLI_CD_MREF)) {
			tempSrvCrdttype = JClaretyConstants.SRV_CRDT_TYP_CD_BRFD;
		}
		bESrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNMbrAcct(pBVAgrmt.mbr_acct, tempSrvCrdttype,null);
		if (errorsPresent()) {
			return null;
		}
		if (bESrvCrdtRef != null) {
			BigDecimal acclPymt = JClaretyConstants.ZERO_BIGDECIMAL;
			int size = 0;
			if(pBVAgrmt.aobepymtinstr != null && pBVAgrmt.aobepymtinstr.size() > 0){
				size = pBVAgrmt.aobepymtinstr.size();
			}
			
			for (int i = 0; i < size; i++) {
				BEPymtInstr bEPymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(i);
				tmpBVAgrmtSmry = new BVAgrmtSmry();
				//If accelerated payment then base cost amount is zero
				if (bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_ACPT)||
						bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_ACRT)) {
					tmpBVAgrmtSmry.base_cost_amt = JClaretyConstants.ZERO_BIGDECIMAL;
				}
				else {
					tmpBVAgrmtSmry.base_cost_amt = bEPymtInstr.orig_amt;
				}
				if (bEPymtInstr.pymt_amt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER) {
					tmpBVAgrmtSmry.finance_amt = JClaretyConstants.ZERO_BIGDECIMAL;
				}
				else {
					if (bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_LSUM)|| 
							bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_RLVR)) {
						tmpBVAgrmtSmry.finance_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					else {
						tmpBVAgrmtSmry.finance_amt = bEPymtInstr.pymt_amt.multiply(new BigDecimal(bEPymtInstr.nbr_of_pymts)).setScale(2, BigDecimal.ROUND_HALF_UP);
						tmpBVAgrmtSmry.finance_amt = tmpBVAgrmtSmry.finance_amt.subtract(bEPymtInstr.orig_amt).setScale(2, BigDecimal.ROUND_HALF_UP);
					}
				}
				if (bEPymtInstr.fnc_item_id != null && bEPymtInstr.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
					tmpBEFncItem = new BEFncItem();
					tmpBEFncItem.fnc_item_id = bEPymtInstr.fnc_item_id.intValue();
					tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
					if (errorsPresent()) {
						return null;
					}
					if (tmpBEFncItem != null) {
						item_amt = tmpBEFncItem.to_alloc_amt;
						tmpBVAgrmtSmry.bvpymtinstr.befncitem = tmpBEFncItem;
					}
				}
				else {
					item_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					tmpBVAgrmtSmry.bvpymtinstr.befncitem = null;
				}

				BEDocStatRef beDocStatRef = null;
				beDocStatRef = dbdocstatref.readByGrpTypNStatCd(JClaretyConstants.DOC_GRP_TYP_CD_OSC,pBVAgrmt.agrmt_stat_cd);
				if (errorsPresent()) {
					return null;
				}
				if (beDocStatRef != null) {
					if (beDocStatRef.doc_stat_grp_cd.equals(JClaretyConstants.DOC_STAT_GRP_CD_COMP)) {
						tmpBVAgrmtSmry.balance_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					else {
						tmpBVAgrmtSmry.balance_amt = bEPymtInstr.pymt_amt.multiply(new BigDecimal(bEPymtInstr.nbr_of_pymts)).setScale(2, BigDecimal.ROUND_HALF_UP);
						tmpBVAgrmtSmry.balance_amt = tmpBVAgrmtSmry.balance_amt.subtract(item_amt).setScale(2, BigDecimal.ROUND_HALF_UP);
					}
				}


				tmpBVAgrmtSmry.paid_to_dt = item_amt;

				if (bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_INST)) {
					tmpBVAgrmtSmry.inst_pymt_amt = bEPymtInstr.pymt_amt;
				}
				else if (bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_PYRL)) {
					tmpBVAgrmtSmry.pyrl_pymt_amt = bEPymtInstr.pymt_amt;
				}
				else if (bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_ACRT)|| 
						bEPymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_ACPT)) {
					//this is accelerated amount by the member,is not payment amount 
					//no need to be mapped 
					//continue
					acclPymt = acclPymt.add(item_amt);
				}
				else {
					tmpBVAgrmtSmry.pyrl_pymt_amt = bEPymtInstr.pymt_amt;
				}
				tmpBVAgrmtSmry.payer_typ = bEPymtInstr.pyr_typ.trim();
				tmpBVAgrmtSmry.bvpymtinstr.bepymtinstr = bEPymtInstr;
				pBVAgrmt.aobvagrmtsmry.add(tmpBVAgrmtSmry);
			}

			int size1 = pBVAgrmt.aobvagrmtsmry.size();
			for (int i = 0; i < size1; i++) {
				BVAgrmtSmry bvAgrmtSmry = (BVAgrmtSmry) pBVAgrmt.aobvagrmtsmry.get(i);
				if (tmpBVAgrmtSmry.equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)) {
					if (bvAgrmtSmry.bvpymtinstr.bepymtinstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_MPYR)) {
						bvAgrmtSmry.balance_amt = bvAgrmtSmry.balance_amt.subtract(acclPymt).setScale(2, BigDecimal.ROUND_HALF_UP);
					}
				}
				else {
					bvAgrmtSmry.balance_amt = bvAgrmtSmry.balance_amt.subtract(acclPymt).setScale(2, BigDecimal.ROUND_HALF_UP);
				}
			}

		}
		addTraceMessage("getBVAgrmtSmry", FABM_METHODEXIT);
		takeBenchMark("getBVAgrmtSmry", FABM_METHODEXIT);
		return pBVAgrmt;
	}
	/*********************************************************
	 * Method Name : calculateRfndBuybackCost()
	 *
	 * Input Parameters	:
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *	P_BEPln : BE_Pln
	 *	P_GetFullBuyBack : Boolean
	 *	P_PostSrvCrdt : Boolean
	 *	P_BVOSCRqstInfo : BV_OSCRqstInfo
	 *	P_MergeSSN : Boolean = FALSE
	 *	
	 * Output Parameters	:
	 *	P_BVOSCRqstInfo : BV_OSCRqstInfo
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method calculates the base cost and the interest amount for all the refunds that occurred at 
	 *	one time. This method is also used by the Web for the Refund Buyback for school members. By default 
	 *	if this method called from the Web for school plan, it defaults to Rate of Return calculation. 
	 *	Rule 1: If refund buyback  type Voluntary Refund, for School plan can be calculated either on Ror or 
	 *	Interest way
	 *	Rule 2: If refund buyback type Voluntary Refund for Judges or Patrols plan, the calculation type is 
	 *	always Interest
	 *	Rule 3: If refund buyback type Voluntary Refund for DC plan then the Total refund Amount is the cost 
	 *	of the refund buyback (always the gross amount)
	 *	Rule 4: If refund buyback type is Mandatory Refund for any plan the Total refund amount is the cost 
	 *	of the refund buyback (always the gross amount)
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 *  NE.01                    skaur2	   04/02/2008  Migration from Forte
	 *  NE.02                    Lashmi    10/14/2008  passed the fndg rqmt's acct_trans_id to
	 *                                                 get the acct_trans object.
	 *  P.01    PIR 77365        la        11/21/2008  set the scale for amt.
	 *  NE.03                   Lashmi     11/25/2008  Work around to fix the error in the calculation
	 *                                                 of interest by FABMMAM.CalcRetroActiveInterest when
	 *                                                 annual interest calculation is involved 
	 * P.02        NPRIS-1194    gpannu    12/09/2009  Added code to get individual wage ineterest detail
	 * 													for refund buy back cases.      
	 * P.03	   NPRIS-1194		gpannu	   01/27/2010  if it is a refund buyback case add one month to becntrb end date 
	 *												   and pass it at as posting date. this is to resolve the problem if cntrbs after 01/01/2003
	 *												   and if jan cntrb is coming in aprl than still we have to calculate 
	 *												   interest from 1 mnth ahead of cntrb end date 
	 * P.04    NPRIS-1194		gpannu	   02/05/2010  Added code to send the additonal interest entered in
	 *  												   adjustment amount field of cost calc tab to calculate interest again with
	 *  												   late int. 
	 * P.05		NPRIS-5272		vgp			08/13/2010 Exclude reversed transactions  	
	 * P.06		NPRIS-5545		vgp			11/09/2011	Exclude unselected refunds for mandatory refund buybacks	
	 * P.07		NPRIS-5564		vgp			01/11/2011	Use FABMMbrAcctMaint.calcInterestEffectiveStartDate()	
	 * P.08		NPRIS-5853		vgp			08/05/2015	Mandatory Refund Buy back proration			
	 * P.09     NPRIS-6407		MKolm       04/24/2017  Fixed cost overstating				   
	 ***********************************************************************************************/
	public BVOSCRqstInfo calculateRfndBuybackCost(BEMbrAcct beMbrAcct, BEPln bePln,boolean pGetFullBuyBack, boolean pPostSrvCrdt,
			BVOSCRqstInfo bvRqstInfo, boolean pMergeSSN)throws ClaretyException {

		addTraceMessage("calculateRfndBuybackCost",FABM_METHODENTRY);
		takeBenchMark("calculateRfndBuybackCost",FABM_METHODENTRY);
		String calcTyp = JClaretyConstants.CLARETY_EMPTY_FIELD;
		BEPlnRef bePlnRef = null;
		int intCalcFlag = JClaretyConstants.ONE_INTEGER;
		//1A@P.01
		BEFscYr  beFscYr = null;
		List aoBEIntRateRef = new ArrayList();
		tpintitem tpintItem = new tpintitem();
		tpintitem tempTp =null;//1A@P.02
		List aoTpList = new ArrayList();//1A@P.02
		BigDecimal tmpAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal qtrlyAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal intAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal cntrbAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal yrlyInt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal yrlyCntrb = JClaretyConstants.ZERO_BIGDECIMAL;
		int fsclYr = 0;
		BigDecimal tempSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BEAcctTrans tmpBEAcctTrans = new BEAcctTrans();
		BigDecimal totSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BVYrlyRfndTrans bvYrlyRfndTransTemp =null;
		bePlnRef = dBPlnRef.readByPlan(bePln, null);
		if (errorsPresent()) {
			return null;
		}
		if (bePlnRef != null) {
			if (bePlnRef.pln_grp_cd.trim().equals(JClaretyConstants.PLN_GRP_CD_DC)) {
				intCalcFlag = JClaretyConstants.ZERO_INTEGER;
			}
			else {
				if (bvRqstInfo.web_in) {
					calcTyp = JClaretyConstants.CALC_TYP_RFNR;
				}else {
					if (bvRqstInfo.cost_calc_typ.trim().equals(JClaretyConstants.COST_CALC_TYP_VRFI)) {
						calcTyp = JClaretyConstants.CALC_TYP_RFNI;
						intCalcFlag = JClaretyConstants.ONE_INTEGER;
					}else if (bvRqstInfo.cost_calc_typ.trim().equals(JClaretyConstants.COST_CALC_TYP_VRFR)) {
						calcTyp = JClaretyConstants.CALC_TYP_RFNR;
						intCalcFlag = JClaretyConstants.ONE_INTEGER;
					}
					//A@P.08 Start
					else if (bvRqstInfo.cost_calc_typ.equalsIgnoreCase(JClaretyConstants.COST_CALC_TYP_MREF) && pPostSrvCrdt) {
						calcTyp = JClaretyConstants.CALC_TYP_RFNI;
						intCalcFlag = JClaretyConstants.ONE_INTEGER;
					} //A@P.08 End
					else {
						intCalcFlag = JClaretyConstants.ZERO_INTEGER;
					}
				}
			}
			if (intCalcFlag == JClaretyConstants.ONE_INTEGER) {
				aoBEIntRateRef = dbIntRateRef.readAllByPlan(bePln, false);
				if(errorsPresent()){
					return null;
				}
				if (aoBEIntRateRef != null) {
					if (bvRqstInfo.aobvrfndtrans != null) {
						int size = bvRqstInfo.aobvrfndtrans.size();
						for (int i = 0; i < size; i++) {
							BVRfndTrans bvRfndTrans = (BVRfndTrans) bvRqstInfo.aobvrfndtrans.get(i);
							if (errorsPresent()) {
								break;
							}
							if (bvRfndTrans.incl_in) {
								if (pGetFullBuyBack) {
									if (bvRfndTrans.aoBVYrlyRfndTrans!= null) {
										int size1 = bvRfndTrans.aoBVYrlyRfndTrans.size();
										for (int j = 0; j < size1; j++) {
											BVYrlyRfndTrans bvYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(j);
											bvYrlyRfndTrans.incl_in = true;
										}
									}
								}
								if (bvRfndTrans.aoBVYrlyRfndTrans!= null) {
									int size2 = bvRfndTrans.aoBVYrlyRfndTrans.size();
									for (int j = 0; j < size2; j++) {
										BVYrlyRfndTrans bvYrlyRfndTrans = (BVYrlyRfndTrans) bvRfndTrans.aoBVYrlyRfndTrans.get(j);
										if (errorsPresent()) {
											break;
										}
										if (bvYrlyRfndTrans.incl_in) {
											yrlyInt = JClaretyConstants.ZERO_BIGDECIMAL;
											yrlyCntrb = JClaretyConstants.ZERO_BIGDECIMAL;
											tempSrv = bvYrlyRfndTrans.srv_crdt_qty;
											totSrv = totSrv.add(tempSrv);
											if(bvYrlyRfndTrans.aobvpyrldtl!= null){
												int size3 = bvYrlyRfndTrans.aobvpyrldtl.size();
												BECntrb becntrb = null;
												for (int k = 0; k < size3; k++) {
													BVPyrlDtl row2 = (BVPyrlDtl) bvYrlyRfndTrans.aobvpyrldtl.get(k);
													//2A@P.04
													if(bvYrlyRfndTrans.aoDetailList!=null && bvYrlyRfndTrans.aoDetailList.size()>JClaretyConstants.ZERO_INTEGER){
														bvYrlyRfndTransTemp = (BVYrlyRfndTrans) bvYrlyRfndTrans.aoDetailList.get(k);
													}
														
													if(row2 != null && row2.befndgrqmt != null){	
														tmpAmt = row2.befndgrqmt.tot_rqd_amt;
														becntrb = new BECntrb();
														becntrb.cntrb_id = row2.befndgrqmt.cntrb_id;
													}
													//Yearly Running Total of contribution amount	
													yrlyCntrb = yrlyCntrb.add(tmpAmt);
													//Running Total of contribution amount	
													cntrbAmt = cntrbAmt.add(tmpAmt);
													//1M@NE.02
													List aoBEAcctTrans = (ArrayList)dbaccttrans.readByCntrb(becntrb,0);
													if(errorsPresent()){
														return null;
													}
													BEAcctTrans bEAcctTrans = null;
													if(aoBEAcctTrans != null && aoBEAcctTrans.size() > 0){
														for(int a = 0; a < aoBEAcctTrans.size(); a++){
															bEAcctTrans = (BEAcctTrans)aoBEAcctTrans.get(a);
															//2M@P.05
															if(bEAcctTrans != null && bEAcctTrans.cost_schd_id == 0 && bEAcctTrans.rfnd_req_id == 0
																	&& bEAcctTrans.rvrs_trans_id == 0 
																	&& (!bEAcctTrans.acct_trans_typ_cd.trim().equalsIgnoreCase(JClaretyConstants.ACCT_TRANS_TYP_CD_MRFN) //2A@P.09
																			|| bEAcctTrans.orig_trans_id == 0))
															{
																break;
															}
															else
															{
																bEAcctTrans = null;
															}
														}
														
													}
													if(bvRqstInfo.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)){
														// Pass trans_dt for Refund buyback cost
													 if(bEAcctTrans != null)
													 {	
														 //NE.03  starts
														 //For the period at which the interest rate frequency code is annual, interest should be
														 //calculated for the previous year's balance. But the calcRetroactiveInterest method calculates
														 //interest for the current year balance. But the method could not be modified as it is being called 
														 //by ER module also 
														 //Hence , the following work around is being done.
														 // For the period upto the end of fiscal year 1987, interest calculation is annual. Then from
														 // fiscal year 1988, it changes to Quarterly
														 //For taking care of this scenario,do a check for the fiscal year of the transaction date.
														 //if it is less than 1987, add one year to the transaction date
														 //if it is equal to 1987, set the transaction date to the first quarter end date
														 //of 1988 i.e. September 30,1987
														 //D@P.07 Start
//														   beFscYr = dbfscyr.readByPln(bePln);
//															if(errorsPresent()){
//																return null;
//															}
//															int fscYr = beFscYr.computeFiscalYearOfDate(bEAcctTrans.trans_dt);
//															if(errorsPresent()){
//																return null;
//															}
//															if(fscYr == 1987)
//															{
//																bEAcctTrans.trans_dt=getValidateHelper().getDateFormatter().stringToDateMMDDYYYY( "09/30/1987" ); 	
//															}
//															else if (fscYr < 1987)
//															{
//																Calendar transDt = Calendar.getInstance();
//																transDt.setTime(bEAcctTrans.trans_dt);
//																transDt.add(Calendar.YEAR,1);
//																bEAcctTrans.trans_dt = transDt.getTime();
//															  	
//															}
														 //D@P.07 End
															bEAcctTrans.trans_dt = fabmmbracctmaint.calcInterestEffectiveStartDate(bePln, bEAcctTrans.trans_dt);	//1A@P.07
															if(getDateCompare().isSameDay(row2.becntrb.strt_dt, new Date("07/01/2005"))
																	|| getDateCompare().isSameDay(row2.becntrb.strt_dt, new Date("07/01/2004"))){
																int ii=0;
															}
																//P.03 starts
																//if it is a refund buyback case add one month to becntrb end date 
																//and pass it at as posting date. this is to resolve the problem if cntrbs after 01/01/2003
																// and if jan cntrb is coming in aprl than still we have to calculate 
																//interest from 1 mnth ahead of cntrb end date
																if(getDateCompare().isDayOnOrAfter(row2.becntrb.strt_dt, new Date("01/01/2003"))){
																	//1M@P.06
																	IntervalData dayIncrInt = new IntervalData();
																	dayIncrInt.setUnit(0,1,0, 0, 0, 0, 0);
																	Date tempDate = DateUtility.add(row2.becntrb.end_dt, dayIncrInt);
																	//Date NextIntPstngDate = bEAcctTrans.trans_dt;
																	//NextIntPstngDate = DateUtility.add(beCntrb.end_dt, dayIncrInt);
																	int diffInMnths = fabmmbracctmaint.calcDiffInMonths(row2.becntrb.end_dt, bEAcctTrans.trans_dt);
																	if(diffInMnths > JClaretyConstants.TWO_INTEGER){
																		bEAcctTrans.trans_dt = tempDate;
																	}
																}//P.03 ends
																	 //Addition at NE.03 ends
																//3D@P.08
//																tpintItem = fabmmbracctmaint.calcRetroactiveInterest(bePln,row2.becntrb,
//																		bEAcctTrans.trans_dt,
//																		bvRqstInfo.due_dt, bvRfndTrans.rfnd_dt,tmpAmt,null,calcTyp,aoBEIntRateRef,bvYrlyRfndTransTemp);//1M@P.04
													   //A@P.08 Start			
													   if (bvRqstInfo.cost_calc_typ.equalsIgnoreCase(JClaretyConstants.COST_CALC_TYP_MREF)) {
														   //for MREF NPRIS refunded interest only upto end of month prior to previous month.
														   //so calculate interest only up to end of month prior to refund date
														   IntervalData monthsInterval = new IntervalData();
														   monthsInterval.setUnit(0,1,0, 0, 0, 0, 0);
														   Date tempDate = DateUtility.subtract(bvRfndTrans.rfnd_dt, monthsInterval);
														   tpintItem = fabmmbracctmaint.calcRetroactiveInterest(bePln,row2.becntrb,
																	bEAcctTrans.trans_dt,
																	tempDate, bvRfndTrans.rfnd_dt,tmpAmt,null,calcTyp,aoBEIntRateRef,bvYrlyRfndTransTemp);
													   } else {
																
														   tpintItem = fabmmbracctmaint.calcRetroactiveInterest(bePln,row2.becntrb,
																bEAcctTrans.trans_dt,
																bvRqstInfo.due_dt, bvRfndTrans.rfnd_dt,tmpAmt,null,calcTyp,aoBEIntRateRef,bvYrlyRfndTransTemp);
													 	}
													   //A@P.08 End
														if (errorsPresent()) {
															return null;
														}	
														//3A@P.02
														tpintItem.cntrb =tmpAmt;
														tpintItem.srvCrdt = row2.becntrb.srv_eqvt_qty;
														tpintItem.rfnDt = bvRfndTrans.rfnd_dt;
													 } 	
													}
													
													
													//aoTpList.add(tpintItem);
													//Yearly running Total of Interest amount
													yrlyInt = yrlyInt.add(tpintItem.item_amt);
													//P.02:starts
													/*Date preDate =null;
													Date curDate = null;
													if(tempTp!=null && tpintItem!=null){
														 preDate =((tpintitem)tempTp.aotpintitem.get(0)).int_eff_dt;
														 curDate = ((tpintitem)tpintItem.aotpintitem.get(0)).int_eff_dt;
													}
													if(preDate!=null && curDate!=null && getDateCompare().isSameDay(preDate, curDate)
															&& fsclYr == row2.befndgrqmt.fscl_yr_strt_nr){
														for(int b=0;b<tempTp.aotpintitem.size();b++){
															
															((tpintitem)tempTp.aotpintitem.get(b)).item_amt=((tpintitem)tempTp.aotpintitem.get(b)).item_amt.add(((tpintitem)tpintItem.aotpintitem.get(b)).item_amt);
															((tpintitem)tempTp.aotpintitem.get(b)).er_int_amt=((tpintitem)tempTp.aotpintitem.get(b)).er_int_amt.add(((tpintitem)tpintItem.aotpintitem.get(b)).er_int_amt);
															((tpintitem)tempTp.aotpintitem.get(b)).bns_int_rate=((tpintitem)tempTp.aotpintitem.get(b)).bns_int_rate.add(((tpintitem)tpintItem.aotpintitem.get(b)).bns_int_rate);
																
														}
														tempTp.item_amt=tempTp.item_amt.add(tpintItem.item_amt);
														tempTp.cntrb=tempTp.cntrb.add(tpintItem.cntrb);
														tempTp.end_dt=tpintItem.end_dt;
														tempTp.srvCrdt=tempTp.srvCrdt.add(tpintItem.srvCrdt);
//														
													}else{*/
														
														aoTpList.add(tpintItem);
													//}
													
													//tempTp =(tpintitem) aoTpList.get(aoTpList.size()-1);
													//P.02:ends
													if (pPostSrvCrdt) {
														//Tract Interest per contribution. We are going to use tot_paid_amt in be_fndg_rqmt
														//as a place holder for calculated interest
														row2.befndgrqmt.osc_rqd_int_amt = tpintItem.item_amt.setScale(2, ArchitectureConstants.ROUND_DECIMAL_HALF_UP);//1M@P.01
													}
													fsclYr = row2.befndgrqmt.fscl_yr_strt_nr;
												}
												
											}
											bvYrlyRfndTrans.tot_int = yrlyInt;
											bvYrlyRfndTrans.mbr_cntrb = yrlyCntrb;
											bvYrlyRfndTrans.tot_cost = yrlyCntrb.add(yrlyInt);
											//Running Total of Interest amount
											intAmt = intAmt.add(yrlyInt);
										}
									}
								}
							}
						}
					}
				}
			}
			else {
				if(bvRqstInfo.aobvrfndtrans!= null){
					int size = bvRqstInfo.aobvrfndtrans.size();
					for (int i = 0; i < size; i++) {
						BVRfndTrans row = (BVRfndTrans) bvRqstInfo.aobvrfndtrans.get(i);
						if (bvRqstInfo.cost_calc_typ.equalsIgnoreCase(JClaretyConstants.COST_CALC_TYP_MREF) && !row.incl_in) {	//3A@P.06
							continue;
						}
						cntrbAmt = cntrbAmt.add(row.rfnd_amt).subtract(row.int_amt);	//1M@P.08
						intAmt = intAmt.add(row.int_amt);	//1A@P.08
					}
				}
			}
			bvRqstInfo.int_amt = intAmt;
			bvRqstInfo.base_cost = cntrbAmt.add(intAmt);
			bvRqstInfo.aoTpItemAmt = aoTpList;//1A@P.02
			if (totSrv.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) != JClaretyConstants.ZERO_INTEGER) {
				// which is possible only in case of partial refund buyback
				bvRqstInfo.purch_srv_crdt_qty = totSrv;
			}
		}
		addTraceMessage("calculateRfndBuybackCost",FABM_METHODEXIT);
		takeBenchMark("calculateRfndBuybackCost",FABM_METHODEXIT);
		return bvRqstInfo;
	}

	/*********************************************************
	 * Method Name : PostService()
	 *
	 * Input Parameters :
	 *	 P_BEPln 		: 	BE_Pln
	 *	 P_BEMbrAcct	:	        BE_Mbr_Acct
	 *        P_BVAgrmt		: 	BV_Agrmt
	 *	 P_EffDt		:	DateTimeNullable
	 *	 P_PstngDt 		: 	DateTimeData
	 *	 P_HideTransIn 	: 	Boolean = FALSE
	 *    P_TabMaintain 	:   Boolean = FALSE
	 *	 P_MergeSSN		:	Integer = 0
	 *
	 * Output Parameters :
	 *   P_PstdSrvCrdtAmt : DecimalData
	 *
	 * Return Value
	 *	Errorstack
	 *
	 * Purpose :
	 *       Call MAM to post Service when agreement is completed or prorated
	 *
	 * Change History
	 * =================================================================
	 *		  CCR#		Updated		Updated 
	 * Ver	  PIR#		By			On			Description
	 * ====	  ====		========	==========	=======================
	 * NE.01			dpujari		3/04/2008	Initial conversion from forte to java 
	 * NE.02			jmisra		10/20/2008	Modified after code review.
	 * NE.03  72635     vsenthil    10/24/2008  Modified for OSC Batch Job 
	 * NE.04			jmisra		11/04/2008	Modified to deal with ArrayIndexOutOfBounds
	 * 											Exception on tab Payment.
	 * P.01  PIR 77365  la          11/21/2008  corrected the wrongly converted code
	 * P.02  PIR 79391  Lashmi      04/16/2008  Since the discrepantAmt was being subtracted from
	 *                                          the last aoBVPyrlDtl's fndg_rqmt's osc_rqd_int_amt,
	 *                                          as the osc_rqd_int_amt was lesser than discrepantAmt,
	 *                                          it became negative due to which Validation error was thrown
	 *                                          for osc_rqd_int_amt from BEFndgRqmt. Hence fix was made to
	 *                                          subtract discrepantAmt partly from each record of aoBVPyrlDtl
	 *                                          (if the discrepantAmt was greater than osc_rqd_int_amt) 
	 *P.03   79691      Lashmi      06/16/2009  fixed the code conversion issue (wrong conversion from PIONEER) 
	 *P.04   80623      jkaur1      10/26/2009  Add code to remove 'NullPointerException'.  
	 *P.05   77795      singhp      10/28/2009  CCR- NPRIS045 - Make Up proration   
	 *P.06	 77795		gpannu		11/24/2009	modified the code to get the current month last day
	 *											as int efft date. after that reverted the code back to old position
	 *P.07   77795      singhp      12/18/2009  CCR- NPRIS045 - Added check to consider osc_rqd_int_amt for other than make up cases   
	 *P.08   77795      singhp      12/24/2009  CCR- NPRIS045 - Added check for calling calculate retroactive interest for Make up cases only.  
	 *P.09	NPRIS-1194	gpannu		12/29/2009  Code added to put conditions for some code that should only be for MKUP cases and to include only
	 *											those pyrldtls in which cntrbs are also included because prior this ccr transactions were getting selected for whole fsycl yr
	 *											while now with this CCR user can even select or unselect mnthly cntrbs also within any fyscl yr.
	 *P.10	77795		singhp		01/27/2010  The interest GL should follow a different rule. 
	 *											As long as there is money in Pending OSC interest, take it out. 
	 *											If not, take it from late interest bucket.
	 *P.11	77795		singhp		01/29/2010	Fix for adjusting agrmt outstanding amt & GL issue.    
	 *P.12	77795		singhp		02/01/2010	Fix for agrmt pstd int getting saved twice. 
	 *P.13	NPRIS-1194	gpannu		02/08/2010  Modified due to cahnge in calc reto active method, passed and additional parameter.
	 *P.14	NPRIS-1194	gpannu		02/16/2010  Added code to check if the discrepency after adding finance amount is less than finance 
	 *											subtract finace amout with it rather than subtracting it with finance amount.    
	 *P.15	NPRIS-1194	gpannu		02/19/2010  Added code to check if cntrbs which are not included for BRFD cases in tab cost calc than thier agrmnt_id 
	 *                        					must get removed in fndg_rqmt.
	 *P.16	NPRIS-5381	vgp			01/13/2011	Post full service when refund buyback cost - paid amount is within tolerance amount   
	 *P.17  NPRIS-5254	vgp			12/20/2011	Calc interest for judge & judges make up
	 *P.18	NPRIS-5564	vgp			01/03/2011	Determine interest posting date for make up posting
	 *P.19	NPRIS-5628	vgp			06/26/2012	Prorate BRFD if service credit is fully posted
	 *P.20	NPRIS-5578	vgp			06/29/2012	Include OSC late interest in late ineterest report.
	 *P.21	NPRIS-5488	vgp			02/05/2013	Move excess OSC interest to over/under OSC account (2151 to 9332)
	 *P.22	NPRIS-5853	vgp			09/17/2015	Apply excess calculated interest (related to tag P.02) starting from last contribution
	 *P.23	NPRIS-6297	vgp			12/21/2015	Prorate finance cost
	 *P.24	NPRIS-6297	vgp			05/17/2016	Fix finace charge applied to prorated contribution.
	 *P.25	NPRIS-6947	VenkataM	08/03/2021	Added code for prorate cntrbs for 'MREF'
	 *********************************************************************************************************************************************/
	//-----------------------------------------------------------------------------------------------------------------
	// By now agreement has received money and is ready to post service credit to the member's account
	// At first, get the funding requirements that are attached to the agreement
	//
	// For refund buyback calculate buyback cost inclusive of the interest. For full buyback, attaching the interest from the refund date
	// to calc date, to the last contribution.
	//
	// For actuarial services, distribute tot_cost between fundings at the ratio of contrb.srv_crdt_qty/tot_srv_crdt_qty
	//
	// For interest based services
	//		If service credit is LOAI then add pre_tax_amt,er_share of each funding. Er_share should store employer contribution get gets paid by member
	//		Else if service credit is MKUP then consider pre_tax_amt only
	// Add interest to the amount upto calc date. That is the tot_rqd_amt per funding. Clear up other fields of the funding
	//
	// Having decided the tot_rqd_amt, find out the total pre and post tax amount paid, and call transOSCPurchase
	//
	// For MakeUp we need to post late interest from calc date to agreement posting date. Post late interest seperately for that in the member's account at the end
	//-----------------------------------------------------------------------------------------------------------------
	public BigDecimal postService(BEPln pBEPln,BEMbrAcct pBEMbrAcct, BVAgrmt pBVAgrmt,Date pEffDt, Date pPstngDt,boolean pHideTransIn, boolean pTabMaintain,
			int pMergeSSN) throws ClaretyException {
		addTraceMessage("postService", FABM_METHODENTRY);
		startBenchMark("postService", FABM_METHODENTRY);

		BigDecimal pPstdSrvCrdtAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BVFabmOscPopulateRefundBuybackDetail bvFabmOscPopulateRefundBuybackDetail = new BVFabmOscPopulateRefundBuybackDetail();
		BESrvCrdtRef beSrvCrdtRef = null;
		//NE.02	- Begin
		List aoBVPyrlDtl = null;
		BigDecimal tempCost = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoBEIntRateRef = null;
		//NE.02	- End
		Date intCalcdate = null;
		BEOSCRqst beOSCRqst = null;
		tpintitem tpIntItem = null;
		String calcTyp = null;
		BigDecimal pretaxamt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal posttaxamt = JClaretyConstants.ZERO_BIGDECIMAL;
		BEFncDoc tmpBEFncDoc = null;
//		BigDecimal financeCost = JClaretyConstants.ZERO_BIGDECIMAL;	//2D@P.23
//		BigDecimal financeCostPerElem = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal baseAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal intAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal tmpIntAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BEMkupPstng beMkupPstng = null;
		Date calcIntPstngDt = null;	//1M@P.18
		Date intEffDate = null;
		//List aoBVAgrmt = new ArrayList();	//1D@P.18
		BEFscYr beFscYr = null;		//2A@P.18
		BEBusnDt beBusnDt = null;
		/*-----------------------------------------------------------------------------------------------------
								Get pre and post tax amount paid
		---------------------------------------------------------------------------------------------------------*/
		if (pBVAgrmt != null && pBVAgrmt.agrmt != null) {
			if (pBVAgrmt.agrmt.paid_pre_tax_amt != null) {
				pretaxamt = pBVAgrmt.agrmt.paid_pre_tax_amt;
			}
			else {
				pretaxamt = JClaretyConstants.ZERO_BIGDECIMAL;
			}
			if (pBVAgrmt.agrmt.paid_post_tax_amt != null) {
				posttaxamt = pBVAgrmt.agrmt.paid_post_tax_amt;
			}
			else {
				posttaxamt = JClaretyConstants.ZERO_BIGDECIMAL;
			}
			// In case of make up, exclude the employer contribution from the total paid pre tax amount
			if (pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)) {
				BEPymtInstr row = new BEPymtInstr();
				int size = pBVAgrmt.aobepymtinstr.size();
				for (int i = 0; i < size; i++) {
					row = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(i);
					
					if (row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR) || 
							row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR)) {      //1AP.05
					//1D@P.05
					//if (row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR)) {
					//NE.02	- Begin
						if (row.fnc_item_id != null && row.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
					//NE.02	- End
							BEFncItem beFncItem = new BEFncItem();
							beFncItem.fnc_item_id = row.fnc_item_id.intValue();
							//= new(fnc_item_id = row.fnc_item_id.value);
							beFncItem = dbfncitem.readByFncItem(beFncItem);
							if (errorsPresent()) {
								return null;
							}
							if (beFncItem != null) {
								pretaxamt = pretaxamt.subtract(beFncItem.to_alloc_amt);
							}
						}
					}
				}
				baseAmt = pretaxamt;
			}
		}
		//A@P.18 Starts
		beFscYr = dbfscyr.readByPln(pBEPln);
		if (errorsPresent()) {
			return null;
		}

		beBusnDt = dBBusnDt.read();
		if (errorsPresent()) {
			return null;
		}
		//If Posting date is in same month as business date then claculate interest as of end of previous month
		Calendar clndr = Calendar.getInstance();
		if (beBusnDt != null) {
			clndr.setTime(beBusnDt.busn_dt);
		}
		Calendar pstngDtClndr = Calendar.getInstance();
		pstngDtClndr.setTime(pPstngDt);
		if (pstngDtClndr.get(Calendar.YEAR) > clndr.get(Calendar.YEAR)
				|| (pstngDtClndr.get(Calendar.YEAR) == clndr.get(Calendar.YEAR) && pstngDtClndr
						.get(Calendar.MONTH) >= clndr.get(Calendar.MONTH))) {
			pstngDtClndr.add(Calendar.MONTH, -1);
		}
		calcIntPstngDt = beFscYr.getLastDayOfMnth(pstngDtClndr
				.get(Calendar.MONTH)
				+ JClaretyConstants.ONE_INTEGER, pstngDtClndr
				.get(Calendar.YEAR));
		//A@P.18 Ends
		
		beSrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNMbrAcct(pBEMbrAcct,pBVAgrmt.agrmt.srv_crdt_typ.trim(),pEffDt);
		if (errorsPresent()) {
			return null;
		}
		/*--------------------------------------------------------------------------------------------------------
							Get funding requirements and populate tot_rqd_amt in each funding
		---------------------------------------------------------------------------------------------------------*/
		if (beSrvCrdtRef != null) {
			aoBVPyrlDtl = dbfndgrqmt.readBVPyrlDtlByAgrmt(pBVAgrmt.agrmt, pBEMbrAcct.acct_id);
			if (errorsPresent()) {
				return null;
			}
		}
		//1A@P.15
		List removeAoBVpyrlDtl = new ArrayList();
		if (aoBVPyrlDtl != null && aoBVPyrlDtl.size() > JClaretyConstants.ZERO_INTEGER) {
		//NE.02	- Begin
			if (beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_RFND)) {
				//This part for Refund Buyback is changes for P.02. For prior flow, refer to PostServiceRem_OLD
				BVOSCRqstInfo bvOSCRqstInfo = null;
				List aoBVYrlyRfndTrans = null;
		//NE.02	- End				
				beOSCRqst = dboscrqst.readbyOSCAgrmt(pBVAgrmt);
				if (errorsPresent()) {
					return null;
				}
				if (beOSCRqst != null) {
					bvOSCRqstInfo = new BVOSCRqstInfo();  
					bvOSCRqstInfo.setParentAttribute(beOSCRqst);
					// Get buyback information
					bvFabmOscPopulateRefundBuybackDetail = this.populateRefundBuybackDetail(pBEMbrAcct,bvOSCRqstInfo,pBVAgrmt.agrmt);
					if(errorsPresent()){
						return null;
					}
					bvOSCRqstInfo = bvFabmOscPopulateRefundBuybackDetail.bvOSCRqstInfo;
					// Calculate buyback cost including interest
					if (bvOSCRqstInfo.aobvrfndtrans != null && bvOSCRqstInfo.aobvrfndtrans.size() > JClaretyConstants.ZERO_INTEGER) {
						bvOSCRqstInfo = this.calculateRfndBuybackCost(pBEMbrAcct, pBEPln,false, true,bvOSCRqstInfo,false);
						if(errorsPresent()){
							return null;
						}
						
						
					}
					aoBVPyrlDtl.clear();
					BVRfndTrans bvrfndtrans = null;
					BVYrlyRfndTrans bvYrlyRfndTrans = null;
					BVPyrlDtl bvPyrlDtl = null;
					int size = bvOSCRqstInfo.aobvrfndtrans.size();
					for (int j = 0; j < size; j++) {//For row in bvOSCRqstInfo.aoBVRfndTrans do
						bvrfndtrans = (BVRfndTrans) bvOSCRqstInfo.aobvrfndtrans.get(j);
						int size1 = bvrfndtrans.aoBVYrlyRfndTrans.size();
						for (int i = 0; i < size1; i++) {//For row1 in row.aoBVYrlyRfndTrans do
							bvYrlyRfndTrans = (BVYrlyRfndTrans) bvrfndtrans.aoBVYrlyRfndTrans.get(i);
							if (bvYrlyRfndTrans.incl_in) {
								int size2 = bvYrlyRfndTrans.aobvpyrldtl.size();
								//9A@P.09
								for (int k = 0; k < size2; k++) {//For row2 in row1.aoBVPyrlDtl do
									
									//added to include only those individual pyrls selected during cost calc i.e refund buy back
									if(
									bvYrlyRfndTrans.aoDetailList!=null && bvYrlyRfndTrans.aoDetailList.size()>JClaretyConstants.ZERO_INTEGER
									&& ((BVYrlyRfndTrans)bvYrlyRfndTrans.aoDetailList.get(k)).incl_in == true){
										bvPyrlDtl = (BVPyrlDtl) bvYrlyRfndTrans.aobvpyrldtl.get(k);
										aoBVPyrlDtl.add(bvPyrlDtl);
									}else if(bvYrlyRfndTrans.aoDetailList==null || bvYrlyRfndTrans.aoDetailList.size()==JClaretyConstants.ZERO_INTEGER){
										bvPyrlDtl = (BVPyrlDtl) bvYrlyRfndTrans.aobvpyrldtl.get(k);
										aoBVPyrlDtl.add(bvPyrlDtl);
									}
								}
							}else{//5A@P.15
								//add those pyrls which are not included for the agreement and remove agrmt_id later
								int size2 = bvYrlyRfndTrans.aobvpyrldtl.size();
								for (int k = 0; k < size2; k++) {
									bvPyrlDtl = (BVPyrlDtl) bvYrlyRfndTrans.aobvpyrldtl.get(k);
									removeAoBVpyrlDtl.add(bvPyrlDtl);
									
								}
							}
						}
					}
				}
			}
			//NE.02	- Begin
			else if (beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_ACTR)) {
			//NE.02	- End
				BigDecimal ratio = JClaretyConstants.ZERO_BIGDECIMAL;
				BVPyrlDtl row = null;
				int size = aoBVPyrlDtl.size();
				for (int j = 0; j < size; j++) {
					row = (BVPyrlDtl) aoBVPyrlDtl.get(j);
					ratio = row.becntrb.srv_eqvt_qty.divide(pBVAgrmt.agrmt.srv_crdt_qty,2,BigDecimal.ROUND_HALF_UP);
					row.befndgrqmt.tot_rqd_amt = pBVAgrmt.agrmt.base_cost.multiply(ratio);
					tempCost = tempCost.add(row.befndgrqmt.tot_rqd_amt);
				}
				// Add up any residual amount to the first funding
				((BVPyrlDtl) aoBVPyrlDtl.get(0)).befndgrqmt.tot_rqd_amt = ((BVPyrlDtl) aoBVPyrlDtl.get(0)).befndgrqmt.tot_rqd_amt
				.add((pBVAgrmt.agrmt.base_cost.subtract(tempCost)));
			}
			//NE.02	- Begin
			else if (beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_INTR)) {
			//NE.02	- End
				// The fundings at this moment should be reliable so far as contribution amounts are concerned. 
				// So add all the relevant column and store the value in tot_rqd_amt.
				// Also calculate interest amount and add to tot_rqd_amt.
				// At the end clear up other fields
				// Read interest rate 
				aoBEIntRateRef = dbIntRateRef.readAllByPlan(pBEPln, false);
				if (errorsPresent()) {
					return null;
				}
				// In order to calculate interest amount, we need to know the interest calculation date. This date is going to 
				// be stored in be_cost_schd for MKUP. 
				//NE.02	- Begin
				if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
						beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)) {
				//NE.02	- End						
					intCalcdate = pBVAgrmt.cost_schd.calc_start_dt;
					calcTyp = JClaretyConstants.CALC_TYP_MKUP;
				}
				//NE.02	- Begin
				else if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
						beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_LOAI)) {
				//NE.02	- End					
					beOSCRqst = dboscrqst.readbyOSCAgrmt(pBVAgrmt);
					if (errorsPresent()) {
						return null;
					}
					if (beOSCRqst != null) {
						intCalcdate = beOSCRqst.calc_start_dt;
					}
					else {
						intCalcdate = new Date();
					}
					calcTyp = JClaretyConstants.CALC_TYP_LOA;
				}
				
				if (aoBEIntRateRef != null && aoBEIntRateRef.size() > JClaretyConstants.ZERO_INTEGER) {
					BVPyrlDtl row = new BVPyrlDtl();
					int size = aoBVPyrlDtl.size();
					for (int i = 0; i < size; i++) {
						row = (BVPyrlDtl) aoBVPyrlDtl.get(i);   
						row.befndgrqmt.tot_rqd_amt = row.befndgrqmt.tot_rqd_amt.add(row.befndgrqmt.pre_tax_amt);; // member contribution
						//NE.02	- Begin
						if(pBVAgrmt.agrmt_stat_cd.equalsIgnoreCase(JClaretyConstants.AGRMT_STAT_C)){
						//3D@P.17
//						if ((pBEPln.pln_id != JClaretyConstants.PLN_ID_JUDGES && pBEPln.pln_id != JClaretyConstants.PLN_ID_JDGS_7)
//								|| (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
//										!beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP))) {
						//NE.02	- End										
							// Calculate interest amount on the tot_rqd_amt
							//2D@P.05 
							//tpIntItem = fabmmbracctmaint.calcRetroactiveInterest(pBEPln,row.becntrb,row.becntrb.end_dt,intCalcdate,
							//		null,row.befndgrqmt.tot_rqd_amt,null,calcTyp,aoBEIntRateRef);
							
							
							// 1A@P.05  get the pstng date to calculate interest from pstng date to current date
							//4M@P.09
							//code modified so that bemkup posting should only gets populated for MKUP
							//cases else let it happen as it was happening prior to MKUP CCR
							if(beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)){
								beMkupPstng = dbMkupPstng.readMkupPstngByPyrlID(row.becntrb.pyrl_id);
								Date calcMkupPstngDt = fabmmbracctmaint.calcInterestEffectiveStartDate(pBEPln, beMkupPstng.pstng_dt);	//1A@P.18
								tpIntItem = fabmmbracctmaint.calcRetroactiveInterest(pBEPln,row.becntrb,calcMkupPstngDt,calcIntPstngDt,	//1M@P.18
										null,row.befndgrqmt.tot_rqd_amt,null,calcTyp,aoBEIntRateRef,null);//1M@P.13
							}else{
								tpIntItem = fabmmbracctmaint.calcRetroactiveInterest(pBEPln,row.becntrb,row.becntrb.end_dt,calcIntPstngDt,	//1M@P.18
										null,row.befndgrqmt.tot_rqd_amt,null,calcTyp,aoBEIntRateRef,null);//1M@P.13
						
							}
							if (errorsPresent()) {
									return null;
								}
						//}		//1D@P.17
						//NE.02	- Begin
						if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
								beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)
								&& pBVAgrmt.agrmt.mkup_er_override_in.booleanValue() == true) {
						//NE.02	- End
							//3A@P.04
							BigDecimal tmpItemAmt = JClaretyConstants.ZERO_BIGDECIMAL;
							if(tpIntItem != null){
								tmpItemAmt = tpIntItem.item_amt;
							}
							//Employer contribution is overridden, do not chanrge interest to the member/employer
							//1M@P.04
							tmpIntAmt = tmpIntAmt.add(tmpItemAmt);
						}
						//3A@P.05
						if(tpIntItem != null){
							intAmt = intAmt.add(tpIntItem.item_amt);
							intEffDate = ((tpintitem) tpIntItem.aotpintitem.get(tpIntItem.aotpintitem.size() - 1)).int_eff_dt;//1M@P.06		//1M@P.18
						}
					}
						/*3D@P.05 else {//
							row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt.add(tpIntItem.item_amt);
						}  */  
						//NE.02	- Begin
						if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
								beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_LOAI)) {
						//NE.02	- End
							// For LOAI add er_share to the tot_rqd_amt because member pays for it
							row.befndgrqmt.tot_rqd_amt = row.befndgrqmt.tot_rqd_amt.add(row.befndgrqmt.er_share); // member pays er_share, hence we post it to member's account																
						}
						row.befndgrqmt.er_share = JClaretyConstants.ZERO_BIGDECIMAL;
					}
				}
			}
			/***********************************************************************************
			Now that the funding requirements are on hand, let's take care of the penny problems.
			If the payment is thru payroll deduction or Installment, then the payment amount
			might vary a little from the actual amount. Let's change the worth of the funding(s)
			by that amount
			 ************************************************************************************/
			// For ACTR the fundings are populated right here. So there is no way we can get any penny problem.
			// Avoid doing the following for ACTR
			//NE.02	- Begin
			if (beSrvCrdtRef != null && !beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_ACTR)) {
			//NE.02	- End
				BigDecimal discrepantAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal toBePaidAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal savedAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				BEPymtInstr row = new BEPymtInstr();
				int size = pBVAgrmt.aobepymtinstr.size();
				for (int i = 0; i < size; i++) {
					row = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(i);
					//NE.02	- Begin
					if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd != null && 
							beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)
							&& (row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_EPYR) 
							|| row.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR))) {// 1A@P.05
					//NE.02	- End							
						continue;
					}
					toBePaidAmt = toBePaidAmt.add(row.orig_amt);
				}
				BVPyrlDtl row1 = null;
				int size1 = aoBVPyrlDtl.size();
				for (int i = 0; i < size1; i++) {
					row1 = (BVPyrlDtl) aoBVPyrlDtl.get(i);
					//NE.02	- Begin
					savedAmt = savedAmt.add(row1.befndgrqmt.tot_rqd_amt);//3A@P.07
					if(!beSrvCrdtRef.srv_crdt_typ_cd.trim().equalsIgnoreCase((JClaretyConstants.SRV_TYP_CD_MKUP))){
						savedAmt = savedAmt.add(row1.befndgrqmt.osc_rqd_int_amt);
						//System.out.println(row1.befndgrqmt.tot_rqd_amt.toString() + " " + row1.befndgrqmt.osc_rqd_int_amt.toString());
					}
					//1D@P.05 savedAmt = savedAmt.add(row1.befndgrqmt.osc_rqd_int_amt); //1M@Cola2OS)C
					//NE.02	- End
				}
				if (savedAmt.compareTo(toBePaidAmt) != JClaretyConstants.ZERO_INTEGER) {
					discrepantAmt = savedAmt.subtract(toBePaidAmt);
					//NE.04 - Begin : 
					/*Second condition is added to deal with ArrayIndexOutOfBounds Exception.
					 * */
					if (pBVAgrmt.agrmt.base_cost_override_in == false && aoBVPyrlDtl.size() > JClaretyConstants.ZERO_INTEGER) {
					//NE.04 - End
						/*D@P.02 starts
						((BVPyrlDtl) aoBVPyrlDtl.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt = ((BVPyrlDtl) aoBVPyrlDtl
								.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt.subtract(discrepantAmt);
								D@P.02 ends*/
						//A@P.02 starts
						//for(int i = 0; i < size1; i++)	//1D@P.22
						for (int i= size1-1; i >= 0; i--)	//1A@P.22
						{
							row1 = null;
							row1 = (BVPyrlDtl) aoBVPyrlDtl.get(i);
							if(row1.befndgrqmt.osc_rqd_int_amt.compareTo(discrepantAmt) >= 0)
							{
								row1.befndgrqmt.osc_rqd_int_amt = row1.befndgrqmt.osc_rqd_int_amt.subtract(discrepantAmt);
								break;
							}
							else
							{
								//since the discrepant amt is greater than osc_Rqd_int_amt, the value equivalent to
								//osc_rqd_int_amt is subtracting from the fndg record.
								//Corresponding the subtracted value is also subtracted from discrepantAmt
								discrepantAmt = discrepantAmt.subtract(row1.befndgrqmt.osc_rqd_int_amt);
								row1.befndgrqmt.osc_rqd_int_amt = JClaretyConstants.ZERO_BIGDECIMAL;	//1A@P.22
								//row1.befndgrqmt.osc_rqd_int_amt = row1.befndgrqmt.osc_rqd_int_amt.subtract(row1.befndgrqmt.osc_rqd_int_amt);	//1D@P.22
								
							}	
						}
						//A@P.02 ends
					}
				}
			}
			//D@P.23 Start
			//@P.23 Finance charge will distributed after determing whether to prorate or not 
//			// At the end distribute financing cost across all funding requirements
//			if (pBVAgrmt.agrmt.base_cost.compareTo(pBVAgrmt.agrmt.tot_cost) != JClaretyConstants.ZERO_INTEGER) {
//				financeCost = pBVAgrmt.agrmt.tot_cost.subtract(pBVAgrmt.agrmt.base_cost);
//				financeCostPerElem = financeCost.divide(new BigDecimal(aoBVPyrlDtl.size()),2, BigDecimal.ROUND_HALF_UP);
//				BigDecimal tmpamt = JClaretyConstants.ZERO_BIGDECIMAL;
//				BVPyrlDtl row = null;
//				int size = aoBVPyrlDtl.size();
//					for (int j = 0; j < size; j++) {
//						row = (BVPyrlDtl) aoBVPyrlDtl.get(j);
//						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt.add(financeCostPerElem);
//						tmpamt = tmpamt.add(financeCostPerElem);
//					}
//				// Add any residual amount to the last funding
//				//NE.04 - Begin : 
//				/*Second condition is added to deal with ArrayIndexOutOfBounds Exception.
//				 * */
//				if (tmpamt.compareTo(financeCost) != JClaretyConstants.ZERO_INTEGER && aoBVPyrlDtl.size() > JClaretyConstants.ZERO_INTEGER) {
//				//NE.04 - End
//					/****** D@P.02 *****************
//					aoBVPyrlDtl[aoBVPyrlDtl.Items].BEFndgRqmt.tot_rqd_amt.value = aoBVPyrlDtl[aoBVPyrlDtl.Items].BEFndgRqmt.tot_rqd_amt.value +
//																			  (tmpamt.value - FinanceCost.value);
//					 ********************************/
//					//Added code to check if the discrepency after adding finance amount is less than finance 
//					//subtract finace amout with it rather than subtracting it with finance amount.
//					//1A@P.14
//					if(tmpamt.compareTo(financeCost) > JClaretyConstants.ZERO_INTEGER ){
//					
//					((BVPyrlDtl) aoBVPyrlDtl.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt = ((BVPyrlDtl) aoBVPyrlDtl
//							.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt.add((tmpamt.subtract(financeCost)));
//					}else{
//						//2A@P.14
//					((BVPyrlDtl) aoBVPyrlDtl.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt = ((BVPyrlDtl) aoBVPyrlDtl
//							.get(aoBVPyrlDtl.size() - 1)).befndgrqmt.osc_rqd_int_amt.add((financeCost.subtract(tmpamt)));
//				
//					}
//				}
//			} //end if 
			//D@P.23 End
		} 
		/*-----------------------------------------------------------------------------------------------------
		Find out if agreement needs to be prorated or not, even if memebr has paid less amount of money in comparison
		to the buyback details from member account
		--------------------------------------------------------------------------------------------------------*/
		boolean continueWOProrate = false;
		boolean proratedAgrmt = true;
		// -- The following addition is to make sure that we do not prorate an agreement if base
		//cost is overridden to a lesser amount in comparison to PIONEER calculated value
		if (pBVAgrmt != null) {
			if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C)|| // Completed
					pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC)|| // Completed from PMA
					pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_T)) { // Transferred
				if (pBVAgrmt.agrmt.base_cost_override_in == true) {
					proratedAgrmt = false;
					//Special treatment to air-time agreement, whereby we would like to find out the status prior to 
					//transfer. If the agreement is completed then we should continue without prorating service
					if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_T)) {
						List docStatHistArray = new ArrayList();
						docStatHistArray = dbdocstathist.readStatHistByFncDoc(pBVAgrmt.agrmt);
						if (errorsPresent()) {
							return null;
						}
						if (docStatHistArray != null && docStatHistArray.size() > JClaretyConstants.ZERO_INTEGER) {
							int size = docStatHistArray.size();
							for (int i = 0; i < size; i++) {//For i in 1 to DocStatHistArray.Items do
								BEDocStatHist beDocStatHist = (BEDocStatHist) docStatHistArray.get(i);
								if (i != JClaretyConstants.ZERO_INTEGER && ((BEDocStatHist) docStatHistArray.get(i)).doc_stat_cd
										.trim().equals(JClaretyConstants.AGRMT_STAT_T)) {
									if (((BEDocStatHist) docStatHistArray.get(i - 1)).doc_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_AR)) {
										proratedAgrmt = true;
										break;
									}
								}
							}
						}
					}
				}
			}
			if (!proratedAgrmt) {
				continueWOProrate = true;
			}
		}
		
		//A@P.23 Start
		//moved here as P.23 requires finace charge calculated based on amortization schedule and prorating or not.
		if (aoBVPyrlDtl != null
				&& pBVAgrmt.agrmt.base_cost.compareTo(pBVAgrmt.agrmt.tot_cost) != JClaretyConstants.ZERO_INTEGER) {
			BigDecimal financeCost = pBVAgrmt.agrmt.tot_cost
					.subtract(pBVAgrmt.agrmt.base_cost);
			BigDecimal financeCostPerElem = JClaretyConstants.ZERO_BIGDECIMAL; 
			BVPyrlDtl row = null;
			Boolean equallyDistributeFinaceCharge = Boolean.TRUE;

			// if voluntary refund buyback and we prorating then finance charge
			// need to be applied based on amortization schedule
			if ((!continueWOProrate)
					&& beOSCRqst != null
					&& beSrvCrdtRef != null
					&& beSrvCrdtRef.schd_typ_cd.trim().equals(
							JClaretyConstants.SCHD_TYP_CD_RFND)
					&& pBVAgrmt.agrmt.tot_cost.compareTo(pretaxamt.add(
							posttaxamt).add(
							JClaretyConstants.BIG_DECIMAL_OSC_TOLERANCE_AMT)) > JClaretyConstants.ZERO_INTEGER
					&& (!beOSCRqst.cost_calc_typ.trim().equals(
							JClaretyConstants.SRV_CRDT_CLI_CD_MREF))
					&& financeCost.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {

				BEIntRateRef beIntRateRef = dbIntRateRef
						.readByMaxEffDtNPlnIdNIntRateTypCd(beOSCRqst.eff_dt,
								pBEPln.pln_id,
								JClaretyConstants.INT_RATE_TYP_CD_AMRT.trim());
				if (errorsPresent()) {
					return null;
				}

				// Calculate finance charge if we find at least one payment
				// instruction with payroll or installment payment
				BEPymtInstr bePymtInstr = null;
				if (pBVAgrmt.aobepymtinstr != null) {
					for (int pymt = 0; pymt < pBVAgrmt.aobepymtinstr.size(); pymt++) {
						bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr
								.get(pymt);
						if (bePymtInstr != null
								&& (bePymtInstr.pymt_typ
										.trim()
										.equalsIgnoreCase(
												JClaretyConstants.PYMT_TYP_INST) || bePymtInstr.pymt_typ
										.trim()
										.equalsIgnoreCase(
												JClaretyConstants.PYMT_TYP_PYRL))) {

							// recalculate finance charge for how much
							// money/number of installments paid.
							// we don't want to charge them full finance charge
							// unless they paid in full
							BigDecimal proratedFinanceCost = this
									.calculateFinanceChargeForPaidAmount(
											pretaxamt.add(posttaxamt),
											pBVAgrmt.agrmt.base_cost,
											beIntRateRef.int_rate_pct,
											bePymtInstr.pymt_amt);
							financeCost = financeCost.doubleValue() < proratedFinanceCost
									.doubleValue() ? financeCost
									: proratedFinanceCost;
							
							equallyDistributeFinaceCharge = Boolean.FALSE;
							break;
						}
					}
				}
			}
			// the following should be executed only if finance charge is not
			// allocated above
			if (equallyDistributeFinaceCharge) {
				BigDecimal totalFinanceChargeApplied = JClaretyConstants.ZERO_BIGDECIMAL;
				financeCostPerElem = financeCost.divide(new BigDecimal(aoBVPyrlDtl.size()), 2,
						BigDecimal.ROUND_HALF_UP);
				for (int j = 0; j < aoBVPyrlDtl.size(); j++) {
					row = (BVPyrlDtl) aoBVPyrlDtl.get(j);
					totalFinanceChargeApplied = totalFinanceChargeApplied
					.add(financeCostPerElem);
					if (totalFinanceChargeApplied.compareTo(financeCost) > JClaretyConstants.ZERO_INTEGER) {
						totalFinanceChargeApplied = totalFinanceChargeApplied
						.subtract(financeCostPerElem);
						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt
						.add(financeCost).subtract(
								totalFinanceChargeApplied);
						break;
					} else {
						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt
						.add(financeCostPerElem);
					}
				}
			} else {
				
				BigDecimal totalFinanceChargeApplied = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal totalPrincipalToWhichFinaceChargeApplied = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal currentPyrlPrincipal = JClaretyConstants.ZERO_BIGDECIMAL;
				//paid principal is paid amount minus calculate finance cost
				BigDecimal totPrincipalPaid = pretaxamt.add(posttaxamt).subtract(
						financeCost);

				for (int j = 0; j < aoBVPyrlDtl.size(); j++) {
					row = (BVPyrlDtl) aoBVPyrlDtl.get(j);
					currentPyrlPrincipal = row.befndgrqmt.osc_rqd_int_amt
							.add(row.befndgrqmt.tot_rqd_amt);
					financeCostPerElem = JClaretyConstants.ZERO_BIGDECIMAL;
					totalPrincipalToWhichFinaceChargeApplied = totalPrincipalToWhichFinaceChargeApplied
							.add(currentPyrlPrincipal);
					//if we allocated all paid principal then end the process
					//Eg. total agreement amount is $500 and paid only $400 
					if (totalPrincipalToWhichFinaceChargeApplied
							.compareTo(totPrincipalPaid) >= JClaretyConstants.ZERO_INTEGER) {
						if (financeCost.compareTo(totalFinanceChargeApplied) > JClaretyConstants.ZERO_INTEGER) {
							financeCostPerElem = financeCost
									.subtract(totalFinanceChargeApplied);
						}
					} else {
						financeCostPerElem = currentPyrlPrincipal
								.multiply(financeCost).divide(totPrincipalPaid,2,BigDecimal.ROUND_HALF_UP);
								
					}
					// P.24 Note: in
					// com.covansys.jclarety.account.app.internal.fabm.FABMMbrAcctMaint.prorateCntrb
					// the contrbution that is being prorated will be apply
					// interest and contribution as a ratio of total.
					// But we need to apply the full left over finance charge
					// and then apply the int and cntrb in proportion
					// But since we add finace charge to osc_rqd_int_amt here in
					// FABMMbrAcctMaint.prorateCntrb we have no
					// way to know how much is finace charge. So we save it in
					// .BVPyrlDtl.financeCharge here and then
					// get passed it ot FABMMbrAcctMaint.transOSCPurchase and
					// then from there to FABMMbrAcctMaint.prorateCntrb
					// Best Way to Handle this: Add a new field in be_fndg_rqrmt
					// and then save it sepertely and use that as
					// other money fields in calculations.
					if (financeCost.compareTo(totalFinanceChargeApplied
							.add(financeCostPerElem)) <= JClaretyConstants.ZERO_INTEGER) {
						financeCostPerElem = financeCost
								.subtract(totalFinanceChargeApplied);
						totalFinanceChargeApplied = financeCost;
						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt
								.add(financeCostPerElem);
						row.financeCharge = financeCostPerElem; // 1A@P.24
						break;
					} else {
						totalFinanceChargeApplied = totalFinanceChargeApplied
								.add(financeCostPerElem);
						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt
								.add(financeCostPerElem);
						row.financeCharge = financeCostPerElem; // 1A@P.24
					}
				}
			}
		}
		//A@P.23 end
		
//		//D@P.19 Start
//		BigDecimal totBuybackCost = JClaretyConstants.ZERO_BIGDECIMAL;	//1D@P.19
//		TPAcctTransId notUsedCntrb = null;
//		List aoNotUsedCntrb = new ArrayList();
//		ArrayList aoNotUsedFndg = new ArrayList();
//		//NE.02	- Begin
//		if (beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd != null && 
//				beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_RFND)) {
//		//NE.02	- End
//			List tempaoBVPyrlDtl = new ArrayList();
//			BVPyrlDtl row = null;
//			int size = aoBVPyrlDtl.size();
//			for (int k = 0; k < size; k++) {
//				row = (BVPyrlDtl) aoBVPyrlDtl.get(k);
//				//If a refund buyback agreemnt is prorated then we can only post integral years and refund the remaining amount to the member
//				//For example, suppose a buyback costs $10,000.00 for 5 years. Member pays $9500 and defaults. With $9500, 3.5 years 
//				//can be bought back. Under this circumstance, we will arrange to post only 3 years (say for $9000.00) and rest of the money ($500)
//				//is going to be refunded to the member.
//				//In order to achieve that, we rearrange aoBVPyrlDtl and send as many years as can be bought by the paid amount
//				//However, if base cost is overridden
//				//row.befndgrqmt.tot_rqd_amt = row.befndgrqmt.tot_rqd_amt.add(row.befndgrqmt.osc_rqd_int_amt);//2D@P.01
//				//totBuybackCost = totBuybackCost.add(row.befndgrqmt.tot_rqd_amt);
//				totBuybackCost = totBuybackCost.add(row.befndgrqmt.tot_rqd_amt.add(row.befndgrqmt.osc_rqd_int_amt));//1A@P.01
//				if (totBuybackCost.compareTo((pretaxamt.add(posttaxamt))) <= JClaretyConstants.ZERO_INTEGER || continueWOProrate) {
//					tempaoBVPyrlDtl.add(row);
//				//14A@P.16
//				//if last payroll record and difference between buy back cost and paid amount is within the tolerance 
//				//then complete the agreement and post the paid money  
//				} else if (k == size - 1
//						&& (totBuybackCost.subtract(pretaxamt.add(posttaxamt))
//								.compareTo(new BigDecimal(OSC_TOLERANCE_AMT))) <= JClaretyConstants.ZERO_INTEGER) {
//					BigDecimal payDiff = totBuybackCost.subtract(pretaxamt.add(posttaxamt));
//					if (row.befndgrqmt.osc_rqd_int_amt.compareTo(payDiff) >= JClaretyConstants.ZERO_INTEGER) {
//						row.befndgrqmt.osc_rqd_int_amt = row.befndgrqmt.osc_rqd_int_amt.subtract(payDiff);
//					} else {
//						payDiff = payDiff.subtract(row.befndgrqmt.osc_rqd_int_amt);
//						row.befndgrqmt.osc_rqd_int_amt = JClaretyConstants.ZERO_BIGDECIMAL;
//						row.befndgrqmt.tot_rqd_amt = row.befndgrqmt.tot_rqd_amt.subtract(payDiff);
//					}
//					tempaoBVPyrlDtl.add(row);
//				}
//				else {
//					notUsedCntrb = new TPAcctTransId();
//					notUsedCntrb.id = row.becntrb.cntrb_id;
//					aoNotUsedCntrb.add(notUsedCntrb);
//					row.befndgrqmt.agrmt_id = JClaretyConstants.ZERO_INTEGER;
//					aoNotUsedFndg.add(row.befndgrqmt);
//				}
//			}
//			aoBVPyrlDtl = tempaoBVPyrlDtl;
//		//D@P.19 End
		//5A@P.19
		if (beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd != null && 
				beSrvCrdtRef.schd_typ_cd.trim().equals(JClaretyConstants.SCHD_TYP_CD_RFND)) {
			List aoNotUsedCntrb = new ArrayList();
			ArrayList aoNotUsedFndg = new ArrayList();
			TPAcctTransId notUsedCntrb = null;
			//added to remove all those cntrbs and fnd_rqmts which are not included for refund. 
			//10A@P.15
			if(removeAoBVpyrlDtl!=null && removeAoBVpyrlDtl.size()>JClaretyConstants.ZERO_INTEGER){
				BVPyrlDtl row = null;	//1A@P.19
				for(int i=0;i<removeAoBVpyrlDtl.size();i++){
					row = (BVPyrlDtl) removeAoBVpyrlDtl.get(i);
					notUsedCntrb = new TPAcctTransId();
					notUsedCntrb.id = row.becntrb.cntrb_id;
					aoNotUsedCntrb.add(notUsedCntrb);
					row.befndgrqmt.agrmt_id = JClaretyConstants.ZERO_INTEGER;
					aoNotUsedFndg.add(row.befndgrqmt);
				}
			}
			if (aoNotUsedCntrb != null && aoNotUsedCntrb.size() > JClaretyConstants.ZERO_INTEGER) {
				if (pBVAgrmt != null) {
					dbfndgrqmt.saveArray(aoNotUsedFndg);
					if (errorsPresent()) {
						return null;
					}
					dbcntrb.deleteCntrbStatArray(aoNotUsedCntrb, pEffDt,pBVAgrmt.agrmt.rcd_crt_nm);
					if (errorsPresent()) {
						return null;
					}
					
					// 3A@P.25
					if(beOSCRqst != null && beOSCRqst.cost_calc_typ.trim().equals(
							JClaretyConstants.SRV_CRDT_CLI_CD_MREF)){
						
						dbcntrb.deleteCntrbStatArray(aoNotUsedCntrb, pEffDt, pBVAgrmt.agrmt.rcd_crt_nm);
						
						if (errorsPresent()) {
							return null;
						}
					}
					
				}
			}
		}
		//	End of A@CCR65
		/*-----------------------------------------------------------------------------------------------------
									Building BE_Acct_Trans (the purchase transaction to be created)
		---------------------------------------------------------------------------------------------------------*/
		BEAcctTrans beAcctTrans = new BEAcctTrans();
		beAcctTrans.setAcctId(pBEMbrAcct.acct_id);
		//NE.02	- Begin
		if(beSrvCrdtRef != null && beSrvCrdtRef.acct_trans_typ_cd != null){
			beAcctTrans.acct_trans_typ_cd = beSrvCrdtRef.acct_trans_typ_cd.trim();
		}
		//NE.02	- End
		int tempInt = JClaretyConstants.ZERO_INTEGER;
		tempInt = pBVAgrmt.cost_schd.cost_schd_id;
		beAcctTrans.setCostSchdId(tempInt);
		tempInt = pBVAgrmt.agrmt.agrmt_id;
		beAcctTrans.agrmt_id = tempInt;
		beAcctTrans.eff_dt = pEffDt;
		beAcctTrans.trans_dt = pPstngDt;
		beAcctTrans.fscl_yr_end_nr = 2000; // years will be fiscal year of trans_dt.
		beAcctTrans.fscl_yr_strt_nr = 2000; // but they must be set to something to get through the validations
		// done the by the TransOSCPurchase before it calls TransOSCPurchaseRem
		beAcctTrans.hide_trans_in = pHideTransIn;
		//NE.02	- Begin
		beAcctTrans.setOrgId(pBVAgrmt.cost_schd.org_id.intValue());
		//NE.02	- End
		/*-----------------------------------------------------------------------------------------------------
										Perform posting
		---------------------------------------------------------------------------------------------------------*/
		// If called by merge SSN then delink the funding requirements from the agreement
		BVPyrlDtl row = null;
		if (pMergeSSN != JClaretyConstants.ZERO_INTEGER) {
			int size = aoBVPyrlDtl.size();
			for (int i = 0; i < size; i++) {
				row.befndgrqmt.agrmt_id = JClaretyConstants.ZERO_INTEGER;
			}
		}
		//NE.02	- Begin
		if(beSrvCrdtRef != null && beSrvCrdtRef.schd_typ_cd != null){
			//8A@P.11
			if(pBVAgrmt != null && pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP) &&  
				pBVAgrmt.agrmtOustStandingAmt != null  && pBVAgrmt.agrmtOustStandingAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
				if(pretaxamt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
					pretaxamt = pretaxamt.add(pBVAgrmt.agrmtOustStandingAmt);
				}else if (posttaxamt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
					posttaxamt = posttaxamt.add(pBVAgrmt.agrmtOustStandingAmt);
				}
			}
			
			beAcctTrans = fabmmbracctmaint.transOSCPurchase(pBEPln, pBEMbrAcct, beAcctTrans,JClaretyConstants.CNTRB_STAT_CD_PSTD,
					aoBVPyrlDtl, beSrvCrdtRef.schd_typ_cd.trim(), pretaxamt, posttaxamt,pTabMaintain, pMergeSSN, pBVAgrmt, //1M@P.02	
					continueWOProrate, proratedAgrmt);
			//8A@P.11
			if(pBVAgrmt != null && pBVAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP) &&  
					pBVAgrmt.agrmtOustStandingAmt != null  && pBVAgrmt.agrmtOustStandingAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
					if(pretaxamt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
						pretaxamt = pretaxamt.subtract(pBVAgrmt.agrmtOustStandingAmt);
					}else if (posttaxamt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
						posttaxamt = posttaxamt.subtract(pBVAgrmt.agrmtOustStandingAmt);
					}
				}
		}
		//NE.02	- End
		if (errorsPresent()) {
			addErrorMsg(1035,new String[]{"Service has not been posted"});
			return null;
		}
		else {
			pPstdSrvCrdtAmt = beAcctTrans.srv_crdt_net_amt;
		}
		
		
		
		
		if (pBVAgrmt.agrmt_stat_cd
				.equalsIgnoreCase(JClaretyConstants.AGRMT_STAT_R)) {// 1AP.08
			for (int i = 0; i < aoBVPyrlDtl.size(); i++) {
				BVPyrlDtl row1 = (BVPyrlDtl) aoBVPyrlDtl.get(i);
				row1.befndgrqmt.tot_rqd_amt = row1.befndgrqmt.tot_rqd_amt
						.add(row1.befndgrqmt.pre_tax_amt); // member
															// contribution
				// NE.02 - Begin
				if (row1.befndgrqmt.acct_trans_id > JClaretyConstants.ZERO_INTEGER) {

					if (beSrvCrdtRef != null
							&& beSrvCrdtRef.srv_crdt_typ_cd != null
							&& beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(
									JClaretyConstants.SRV_TYP_CD_MKUP)) {

						beMkupPstng = dbMkupPstng
								.readMkupPstngByPyrlID(row1.becntrb.pyrl_id);
						if (errorsPresent()) {
							return null;
						}
						Date calcMkupPstngDt = fabmmbracctmaint.calcInterestEffectiveStartDate(pBEPln, beMkupPstng.pstng_dt);	//1A@P.18
						tpIntItem = fabmmbracctmaint.calcRetroactiveInterest(
								pBEPln, row1.becntrb, calcMkupPstngDt,				//1M@P.18
								calcIntPstngDt, null, row1.befndgrqmt.tot_rqd_amt,	//1M@P.18
								null, calcTyp, aoBEIntRateRef,null);//1M@P.13
					}
					if (errorsPresent()) {
						return null;
					}
					// System.out.println("----date----"+beMkupPstng.pstng_dt+"---------amt---------"+row1.befndgrqmt.tot_rqd_amt+"----------"+tpIntItem.item_amt);

					// NE.02 - Begin
					if (beSrvCrdtRef != null
							&& beSrvCrdtRef.srv_crdt_typ_cd != null
							&& beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(
									JClaretyConstants.SRV_CRDT_TYP_CD_MKUP)
							&& pBVAgrmt.agrmt.mkup_er_override_in
									.booleanValue() == true) {
						// NE.02 - End
						// 3A@P.04
						BigDecimal tmpItemAmt = JClaretyConstants.ZERO_BIGDECIMAL;
						if (tpIntItem != null) {
							tmpItemAmt = tpIntItem.item_amt;
						}
						// Employer contribution is overridden, do not chanrge
						// interest to the member/employer
						// 1M@P.04
						tmpIntAmt = tmpIntAmt.add(tmpItemAmt);
					}
					// 3A@P.05 added so that the late interest is getting calculated till size-2.
					if (tpIntItem != null) {
						intAmt = intAmt.add(tpIntItem.item_amt);
						intEffDate = ((tpintitem) tpIntItem.aotpintitem
								.get(tpIntItem.aotpintitem.size() - 1)).int_eff_dt;//1M@P.06	//1M@P.18
					}
				}
			}
		}
		
		
		
		/*-----------------------------------------------------------------------------------------------------
			At the end, for MKUP, arrange to post late interest from calc_date to posting date, if calc_date 
			is less than P_PstngDt and calc_date and P_PstngDt do not lie in the same month and same year
		---------------------------------------------------------------------------------------------------------*/
		//NE.02	- Begin
		if (beSrvCrdtRef != null && beSrvCrdtRef.srv_crdt_typ_cd  != null && 
				beSrvCrdtRef.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_TYP_CD_MKUP)) {
		//NE.02	- End				
			/*38D@P.05  commented because we are getting interest amount from up.  if (getDateCompare().isDayBefore(intCalcdate, pPstngDt)) {
				Calendar calCalcdate = Calendar.getInstance();
				calCalcdate.setTime(intCalcdate);
				int tmpCalMnth = calCalcdate.get(Calendar.MONTH) + 1;
				int tmpCalyr = calCalcdate.get(Calendar.YEAR);
				Calendar calpPstngDt = Calendar.getInstance();
				//NE.02	- Begin
				calpPstngDt.setTime(pPstngDt);
				//NE.02	- End
				int tmpPstrMnth = calpPstngDt.get(Calendar.MONTH) + 1;
				int tmpPstyr = calCalcdate.get(Calendar.YEAR);
				//NE.02	- Begin
				if ((tmpCalMnth == tmpPstrMnth) && (tmpCalyr == tmpPstyr)) {
				//NE.02	- End
					// do not do anything because the interest is already calculated upto the end of the month
					// and charged to the member
				}
				else {
					// The first interest posting cycle after IntCalcDate is already covered. So start from the next cycle					
					// Anyhow the advancement by one cycle wull anyway happen inside
					//NE.02	- Begin
					tpIntItem = fabmmbracctmaint.calcRetroactiveInterest(pBEPln, null,intCalcdate, pPstngDt,null, baseAmt, null,
							calcTyp.trim(), aoBEIntRateRef);
					//NE.02	- End		
					if(errorsPresent()){
						return null;
					}
					// Create transaction to post late interest from IntCalcDate to P_PstngDt
					tpintitem row1 = null;
					int size = tpIntItem.aotpintitem.size();
					for (int k = 0; k < size; k++) { //For row in 	TPIntItem.aoTPIntItem do
						row1 = (tpintitem) tpIntItem.aotpintitem.get(k);
						if (!getDateCompare().isDayAfter(row1.int_eff_dt, pPstngDt)) {
							intAmt = intAmt.add(row1.item_amt);
						}
					}
				}
			}  */
			if (intAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER ||
					tmpIntAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
				//			aoBVIntAmt		: Array of BV_Int_Amt;
				//			BVIntAmt		: BV_Int_Amt;
				Date effDt = null;
				effDt = pEffDt;
				// Create int_amt object
				BVIntAmt bvIntAmt = new BVIntAmt();
				//1M@P.01
				//BVIntAmt.int_amt.value 	= IntAmt.value;
				bvIntAmt.int_amt = intAmt.add(tmpIntAmt);
				bvIntAmt.inv_fnd_sys_cd = JClaretyConstants.ACCT_TRANS_TYP_CD_LINT;
				bvIntAmt.pln_id = pBEPln.pln_id;
				List aoBVIntAmt = new ArrayList();
				aoBVIntAmt.add(bvIntAmt);
				// Create transaction to post late interest worth IntAmt to the memeber's account
				BEAcctTrans pbeAcctTrans = new BEAcctTrans();
				pbeAcctTrans.setAcctId(pBEMbrAcct.acct_id);
				pbeAcctTrans.acct_trans_typ_cd = JClaretyConstants.ACCT_TRANS_TYP_CD_LINT;
				//1D@P.03
				//pbeAcctTrans.eff_dt = ((tpintitem) tpIntItem.aotpintitem.get(tpIntItem.aotpintitem.size() - 1)).int_eff_dt;
				//1A@P.03
				//1D@P.05
				//pbeAcctTrans.eff_dt = ((tpintitem) tpIntItem.aotpintitem.get(tpIntItem.aotpintitem.size() - 2)).int_eff_dt;
				//1A@P.05 
				pbeAcctTrans.eff_dt = intEffDate;
				pbeAcctTrans.trans_dt = pPstngDt;
				pbeAcctTrans.hide_trans_in = pHideTransIn;
				int fsclYrNr = JClaretyConstants.ZERO_INTEGER;
				fsclYrNr = dbfscyr.calculateFiscalYearNr(pBEPln, pbeAcctTrans.trans_dt);
				pbeAcctTrans.fscl_yr_end_nr = fsclYrNr; // years will be fiscal year of trans_dt.
				pbeAcctTrans.fscl_yr_strt_nr = fsclYrNr; // but they must be set to something to get through the validations							
				pbeAcctTrans = fabmmbracctmaint.postInterest(pBEMbrAcct,aoBVIntAmt,pbeAcctTrans.eff_dt,pBEPln,pbeAcctTrans,
						pHideTransIn,pTabMaintain,JClaretyConstants.ACCT_TRANS_TYP_CD_LINT,pMergeSSN, pPstngDt);
				if(errorsPresent()){
					return null;
				}
				//BigDecimal pendingIPYRAmt  = JClaretyConstants.ZERO_BIGDECIMAL;//5A@P.10	//1D@P.21
				BigDecimal IPYRAmt  = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal GLIntAmt  = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal LINTAMT  = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal IntAmt  = JClaretyConstants.ZERO_BIGDECIMAL;
				BigDecimal excessIPYRAmt  = JClaretyConstants.ZERO_BIGDECIMAL;	//1M@P.21
				
				
				
				
				
				
				// Do the little bit of GL for posting the interest, if not from PMA
				BEMbr beMbr = null;
				if (pTabMaintain == false) {
					// From PMA, we do not need any GL
					beMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
					if (errorsPresent()) {
						return null;
					}
					if (beMbr != null) {
						
						//82A@P.10
						 boolean IPYRExists = false;
						 BEAgrmtMbrIntPstdLink beAgrmtMbrIntPstdLink = null;
						 for (int pymt = 0; pymt < pBVAgrmt.aobepymtinstr.size(); pymt++) {
							 BEPymtInstr bePymtInstr = (BEPymtInstr) pBVAgrmt.aobepymtinstr.get(pymt);
							 if (bePymtInstr.pymt_typ.trim().equals(JClaretyConstants.PYMT_TYP_IPYR)){
								 BEFncItem tmpBEFncItem = new BEFncItem();
								 if(bePymtInstr != null && bePymtInstr.fnc_item_id != null){//1A@P.11
									 tmpBEFncItem.fnc_item_id = bePymtInstr.fnc_item_id.intValue();
									 IPYRExists = true;
									 if(tmpBEFncItem != null){
										 tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
										 if (errorsPresent()) {
											 return null;
										 }
										 IPYRAmt = tmpBEFncItem.item_amt;
									 }
								 }
							 }
						 }
						//1M@P.12
						//D4@P.21
//						beAgrmtMbrIntPstdLink = dbagrmt.readMbrIntPstdLinkbyAcctID(pBEMbrAcct);//1M@P.11
//						if(errorsPresent()){
//							return null;
//						}
						
						if(aoBVIntAmt !=  null && aoBVIntAmt.size() > JClaretyConstants.ZERO_INTEGER){
							IntAmt =((BVIntAmt) aoBVIntAmt.get(0)).int_amt;
						}
						LINTAMT = intAmt; 	//1A@P.21
						if(IPYRExists){
							
							if(IntAmt.compareTo(IPYRAmt) <= JClaretyConstants.ZERO_INTEGER){
								GLIntAmt = IntAmt;
								LINTAMT = JClaretyConstants.ZERO_BIGDECIMAL;	//1A@P.21
								excessIPYRAmt = IPYRAmt.subtract(IntAmt);
							}else{
								LINTAMT = IntAmt.subtract(IPYRAmt);
								GLIntAmt = IPYRAmt;
							}
						}	//1A@P.21
						//D@P.21 Start
						//This has been removed as PIR5488 moves all excess interest to 9332 and so if the member ever come back to finish 
						//the rest of agreement we get the money from 9322
//						}else{
//							
//							if(beAgrmtMbrIntPstdLink != null){
//								//5M@P.11
//								if(beAgrmtMbrIntPstdLink.tot_ipyr_amt.compareTo(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd) < JClaretyConstants.ZERO_INTEGER){
//									TotalIPYRAmt = JClaretyConstants.ZERO_BIGDECIMAL;
//								}else{
//									TotalIPYRAmt = beAgrmtMbrIntPstdLink.tot_ipyr_amt.subtract(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd);
//								}
//								
//								if(IntAmt.compareTo(TotalIPYRAmt) <= JClaretyConstants.ZERO_INTEGER){
//										GLIntAmt = IntAmt;
//								}else{
//									LINTAMT = IntAmt.subtract(TotalIPYRAmt);
//									GLIntAmt = TotalIPYRAmt;
//								}
//							}
//						}
						//D@P.21 End
					
						if(GLIntAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
						tmpBEFncDoc = fabmgltrans.postGLForInt(pBEPln,pPstngDt,pBEMbrAcct,GLIntAmt,
								beMbr, pEffDt,false,true);//1M@P.05 setting MakeupLate Interest to true
						if(errorsPresent()){
							return null;
						}
						}
						if(LINTAMT.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
							tmpBEFncDoc = fabmgltrans.postGLForInt(pBEPln,pPstngDt,pBEMbrAcct,LINTAMT,
									beMbr, pEffDt,true,false);// the make UP interest should be false.
							if(errorsPresent()){
								return null;
							}
						}
						//A@P.21 Start
						//PIR5488 -Move any interest that paid but not posted to member account to 9332 
						if(excessIPYRAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
							fabmgltrans.postGLForExcessMakeupInerest(pBEPln,pPstngDt,excessIPYRAmt,
									beMbr, pEffDt);
						}
						//A@P.21 End
						if (pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R) ||
								pBVAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C)) {
						
							
							Date peffDt  = getTimeMgr().getCurrentDateOnly();//3A@P.03
							BigDecimal LINTAmt = JClaretyConstants.ZERO_BIGDECIMAL;
							List aoBEIntItem = new ArrayList();
							//18M@P.11
						//D7@P.21
//							if(beAgrmtMbrIntPstdLink != null){
//								beAgrmtMbrIntPstdLink.tot_mbr_int_pstd = beAgrmtMbrIntPstdLink.tot_mbr_int_pstd.add(((BVIntAmt) aoBVIntAmt.get(0)).int_amt);
//								beAgrmtMbrIntPstdLink = dbagrmt.saveAgrmtMbrIntPstdLink(beAgrmtMbrIntPstdLink);
//								if (errorsPresent()) {
//									return null;
//								}
//							}else{
								
							beAgrmtMbrIntPstdLink = new BEAgrmtMbrIntPstdLink();
							beAgrmtMbrIntPstdLink.agrmt_id = pBVAgrmt.agrmt.agrmt_id;
							beAgrmtMbrIntPstdLink.acct_id = pBEMbrAcct.acct_id;
							beAgrmtMbrIntPstdLink.tot_ipyr_amt = IPYRAmt;
							beAgrmtMbrIntPstdLink.tot_mbr_int_pstd = ((BVIntAmt) aoBVIntAmt.get(0)).int_amt;
							beAgrmtMbrIntPstdLink = dbagrmt.saveAgrmtMbrIntPstdLink(beAgrmtMbrIntPstdLink);
							if (errorsPresent()) {
								return null;
							}
						
//						}	//D1@P.21
						
						}
						//5D@P.10
						/*tmpBEFncDoc = fabmgltrans.postGLForInt(pBEPln,pPstngDt,pBEMbrAcct,((BVIntAmt) aoBVIntAmt.get(0)).int_amt,
								beMbr, pEffDt,false,true);//1M@P.05 setting MakeupLate Interest to true
						if(errorsPresent()){
							return null;
						}*/
					}
				}
				// After everything is done, we want to add a record to tp_late_int_pstg_rpt,
				// so that the money earns interest in this month
				tplateintpstgrpt tpLateIntPstgRpt = new tplateintpstgrpt();
				tpLateIntPstgRpt.late_int_amt = JClaretyConstants.ZERO_BIGDECIMAL;
				tpLateIntPstgRpt.late_cntrb_amt = baseAmt;
				intAmt = intAmt.add(tmpIntAmt);
				//3A@P.20
				if (LINTAMT.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
					intAmt = intAmt.subtract(LINTAMT);
					tpLateIntPstgRpt.late_int_amt = LINTAMT;
				}
				tpLateIntPstgRpt.late_cntrb_amt = tpLateIntPstgRpt.late_cntrb_amt.add(intAmt);
				tpLateIntPstgRpt.late_int_pstd = JClaretyConstants.ZERO_BIGDECIMAL;
				tpLateIntPstgRpt.late_int_billed = JClaretyConstants.ZERO_BIGDECIMAL;
				tpLateIntPstgRpt.acct_id = pBEMbrAcct.acct_id;
				tpLateIntPstgRpt.pln_id = pBEPln.pln_id;
				//NE.02	- Begin
				tpLateIntPstgRpt.wc_rpt_id = JClaretyConstants.ZERO_INTEGER;
				//NE.02	- End
				tpLateIntPstgRpt.eff_dt = beAcctTrans.eff_dt;
				tpLateIntPstgRpt.busn_dt = beAcctTrans.trans_dt;
				dbLateIntPstgRpt.save(tpLateIntPstgRpt);
				if(errorsPresent()){
					return null;
				}
			}
		}
		addTraceMessage("postService", FABM_METHODEXIT);
		startBenchMark("postService", FABM_METHODEXIT);
		return pPstdSrvCrdtAmt;
	}
	/*
	* Description : Check if any previous refund exists for a member
	* Change History
	* ===================================================================================
	*        CCR#            Updated    	  Updated
	* Ver    PIR#              By         		On         Description
	* ====   ===========    ===========  	==========  =============================
	* P.01	 NPRIS-6481		 VenkataM		12/22/2017	  If there is change in tier, comparing the cost factors	
	**************************************************************************************/
	public boolean compareCostTierFactors(BEOSCRqst beOscRqst, BEMbrAcct beMbrAcct) throws ClaretyException {

		FABMPlanTier fabmPlanTier = (FABMPlanTier) getFABMInstance(FABMPlanTier.class);
		BETierRef currentTier = fabmPlanTier.getCurrentPlanTierForMemberAcct(beMbrAcct).beTierRef;

		int currentTierRefId = 0;
		if(currentTier != null){
		currentTierRefId = currentTier.tier_ref_id;
		}
		// If there is a change in tier, comparing the cost tier factors
		if (currentTierRefId > 0 && currentTierRefId != beOscRqst.tier_ref_id) {

			DBCubeFctr dBCubeFctr = (DBCubeFctr) getDBInstance(DBCubeFctr.class);
			DBPrsn dBPrsn = (DBPrsn) getDBInstance(DBPrsn.class);

			BEPrsn bePrsn = (BEPrsn) dBPrsn.readByRetNr(beMbrAcct.ret_nr).get(0);
			Date costCalcDate = beOscRqst.calc_start_dt;
	        // Calculating age as of the calc start date
			int age =  bePrsn.getActuarialAge(costCalcDate);
			//int age = costCalcDay.get(Calendar.YEAR) - birth.get(Calendar.YEAR);

			BigDecimal total_yrs_of_service = JClaretyConstants.ZERO_BIGDECIMAL;
			total_yrs_of_service = beOscRqst.pstd_srv_crdt_qty.add(beOscRqst.proj_srv_qty);

			// Getting the cost tier factor for the current tier
			BigDecimal currentTierCubeCostRate = dBCubeFctr.readCstFctrByAgeNService(age,
					total_yrs_of_service.intValue(), currentTierRefId);
			
			// Getting the cost tier factor for previous tier in which the OSC was purchased
			BigDecimal previousTierCubeCostRate = dBCubeFctr.readCstFctrByAgeNService(age,
					total_yrs_of_service.intValue(), beOscRqst.tier_ref_id);
			
			// Comparing the cost tier
			if(previousTierCubeCostRate.compareTo(currentTierCubeCostRate) < JClaretyConstants.ZERO_INTEGER){
				// Member is in higher cost tier but trying to purchase with the
				// lower cost tier
				return false;
			}
	    }
		return true;
	}
	
	
	/*
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					vkundra     04/08/2008  Initial migration from FORTE
	 * P.01   77367             vsenthil    11/25/2008  Modified the code to prevent acct bal being generated second time for ERBK/IRCR
	 *                                                  after the batch had processed these refunds for an agreement once
	 * P.02	  77795             gpannu		10/20/2009	Added condition to get the balance for Excess Makup Refund 
	 * 													, new refund type added for excess makeup refund  
	 * P.03	  77795             singhp		12/29/2009	Added Check for Null Pointer Exception.
	 * P.04	  NPRIS-5570		MKolm		10/19/2015	Changed from fiscal year to Calendar year for 415 limit
	 **************************************************************************************/
	public BVOSCBal getAgrmtRfndAmt(BERfndReq pBERfndReq,BEPln pBEPln, BEMbrAcct pBEMbrAcct, Date pEffDt)
	throws ClaretyException {
		addTraceMessage("getAgrmtRfndAmt", FABM_METHODENTRY);
		startBenchMark("getAgrmtRfndAmt", FABM_METHODENTRY);
		BVOSCBal bVOSCBal = new BVOSCBal();
		BigDecimal proratePreTax = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal proratePostTax = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal rfndSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal iNSSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal oOSSrv = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal prorateRatio = JClaretyConstants.ZERO_BIGDECIMAL.setScale(8);
		BigDecimal fYAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal tmpFYAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BVIRC415Lmts bVIRC415Lmts = null;
		BEMbr bEMbr = null;
		BEPln bEPln = null;
		List tmpaoBVAgrmt = null;
		List aoBVAgrmt = new ArrayList();
		List tmpaoBVOSCBal = new ArrayList();
		BVOSCBal tmpBVOSCBal = new BVOSCBal();
		List aoBVSrvCrdtCatgAmt = null;
//		int fscYr = 0; //1D@P.04
		BEOSCRqst bEOSCRqst;
		boolean isProrate = false;
		BigDecimal tmpValue = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		//Read member by member account
		bEMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
		if (errorsPresent()) {
			return null;
		}
		//Read pln by mbr acct

		bEPln = dbpln.readByBusnAcct(pBEMbrAcct);
		if(errorsPresent()){
			return null;
		}

		//Select ALL agreements by member account -> Returns array of BV_Agrmt (prorated, completed, in process defereed for IRC415)
		if ( bEMbr != null && bEPln != null) {
			tmpaoBVAgrmt = dbagrmt.readByMbrNPlnNOrgNStatGrp(bEMbr,pBEPln,null,JClaretyConstants.AGRMT_STAT_ALL,pEffDt);//stat_grp_cd = All by default
			if(errorsPresent()){
				return null;
			}
		}
		if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSR)||
				pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSD)) {
			//Get the Service credit by category
			aoBVSrvCrdtCatgAmt = fabmmbracctmaint.readSrvCrdtForTotByCatgNStat(pBEMbrAcct,JClaretyConstants.CNTRB_STAT_CD_PSTD,
					JClaretyConstants.INDICATOR_NEG_ONE,JClaretyConstants.INDICATOR_NEG_ONE,(ArrayList) aoBVSrvCrdtCatgAmt);
			if(errorsPresent()){
				return null;
			}
			//Figure out the total non-out of state service credit in the member's account
			int size = aoBVSrvCrdtCatgAmt.size();
			for (int i = 0; i < size; i++) {
				BVSrvCrdtCatgAmt row = (BVSrvCrdtCatgAmt) aoBVSrvCrdtCatgAmt.get(i);
				if (row.srv_cat_cd.trim().equals(JClaretyConstants.SRV_CAT_CD_OOSC)) {
					oOSSrv = oOSSrv.add(row.srv_eqvt_qty);
				}
				else {
					iNSSrv = iNSSrv.add(row.srv_eqvt_qty);
				}
			}
			//Find out how much of excess out of state SC needs to be refunded
			rfndSrv = oOSSrv.subtract(iNSSrv);
			/*if(rfndSrv.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) <= 0){
				addErrorMsg(1035, new String[] {"There is no Excess OOS Service Credit to be refunded"});
				return null;
			}*/
		}
		if (tmpaoBVAgrmt != null) {
			for (int i = tmpaoBVAgrmt.size() - 1; i >= JClaretyConstants.ZERO_INTEGER; i--) {
				//For excess OOS and AIRT, we care about posted and prorated agreements only. 
				//So get rid of agreements in all other statuses
				BVAgrmt row1 = (BVAgrmt) tmpaoBVAgrmt.get(i);
				if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSR) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSD)) {
					if (!row1.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_OOS)) {
						tmpaoBVAgrmt.remove(i);
					}
					else {
						if (!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_C) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_R) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MR)) {
							tmpaoBVAgrmt.remove(i);
						}
					}
				}
				else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_AIRR) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_AIRD)) {
					if (!row1.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)) {
						tmpaoBVAgrmt.remove(i);
					}
					else {
						if (!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_AC) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_AR)) {
							tmpaoBVAgrmt.remove(i);
						}
					}
				}
				//Must not pocess IRC415 for Refund buyback & makeup
				else if ((pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCR) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCD)) &&
						(row1.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD) ||
								row1.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MKUP))) {
					tmpaoBVAgrmt.remove(i);
				}
				else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBK) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBD)) {
					if (!row1.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)) {
						tmpaoBVAgrmt.remove(i);
					}
					else {
						if (!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_C) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.DOC_STAT_CD_R) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC) &&
								!row1.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MR)) {//Completed or Pro-rated
							tmpaoBVAgrmt.remove(i);
						}
					}
				}
			}
		}

		//5D@P.04
//		//Find out fiscal year of the P_EffDt
//		fscYr = dbfscyr.calculateFiscalYearNr(pBEPln,pEffDt);
//		if(errorsPresent()){
//			return null;
//		}

		//2A@P.04
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(pEffDt);
		
		if ( tmpaoBVAgrmt != null) {
			int size = tmpaoBVAgrmt.size();
			for (int i = 0; i < size; i++) {
				isProrate = false;
				BVAgrmt row = (BVAgrmt) tmpaoBVAgrmt.get(i);
				if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_AIRR) || 
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_AIRD)) {
					bEOSCRqst = dboscrqst.readbyOSCAgrmt(row);
					if(errorsPresent()){
						return null;
					}
					if (bEOSCRqst != null) {
						//If the member promised to retire on eff_rtrmt_dt and that date has yet not come,
						//meaning, it's greater than the P_Effdt, it implies no need to do an air-time refund
						if (bEOSCRqst.eff_rtrmt_dt != null) {
							if (getDateCompare().isDayAfter(bEOSCRqst.eff_rtrmt_dt, pEffDt)){
								continue;
							}
							else {
								aoBVAgrmt = new ArrayList();
								aoBVAgrmt.add(row);
								tmpBVOSCBal = this.readAgrmtBalByAcctNEffDtNCtgry(pBEMbrAcct,pBEPln,JClaretyConstants.CTGRY_PSTD,
										pEffDt,(ArrayList) aoBVAgrmt);
								if(errorsPresent()){
									return null;
								}
								if (tmpBVOSCBal != null) {
									tmpBVOSCBal.bvagrmt = row;
									tmpaoBVOSCBal.add(tmpBVOSCBal);//need to refund
								}
							}
						}
						else {
							addErrorMsg(1035,new String[]{"Retirement date not found"});
							return null;
						}
					}
				}
				else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSR) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_OOSD)) {
					if (rfndSrv.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
						if (rfndSrv.compareTo(row.agrmt.srv_crdt_qty) < JClaretyConstants.ZERO_INTEGER) {
							//Need to be Prorated with Service Credit
							//Use ratio = RfndSrv.Value/row.agrmt.srv_crdt_qty
							isProrate = true;
							tmpValue = rfndSrv;
							rfndSrv = JClaretyConstants.ZERO_BIGDECIMAL;
						}
						else {
							rfndSrv = rfndSrv.subtract(row.agrmt.srv_crdt_qty);
						}
						aoBVAgrmt = new ArrayList();
						aoBVAgrmt.add(row);
						tmpBVOSCBal = this.readAgrmtBalByAcctNEffDtNCtgry(pBEMbrAcct,pBEPln,JClaretyConstants.CTGRY_PSTD,pEffDt,
								(ArrayList) aoBVAgrmt);
						if(errorsPresent()){
							return null;
						}
						if ( tmpBVOSCBal != null) {
							tmpBVOSCBal.bvagrmt = row;
							if (isProrate == true) {
								if (row.agrmt.srv_crdt_qty != null && row.agrmt.srv_crdt_qty
										.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
									//Find out the ratio that needs to be used to pro-ration
									//ProrateRatio.Divide(RfndSrv, row.agrmt.srv_crdt_qty);
									prorateRatio = tmpValue.divide(row.agrmt.srv_crdt_qty,8,ArchitectureConstants.ROUND_DECIMAL_DEFAULT);
									//Prorate the pre-tax using the ratio found above
									proratePreTax = prorateRatio.multiply(row.agrmt.paid_pre_tax_amt);
									//Prorate the post-tax using the ratio found above	
									proratePostTax = prorateRatio.multiply(row.agrmt.paid_post_tax_amt);
									//Update the o/p BV_OSC_Bal with the new amounts calculated here
									tmpBVOSCBal.oscpretax = proratePreTax;
									tmpBVOSCBal.oscposttax = proratePostTax;
								}
							}
							tmpaoBVOSCBal.add(tmpBVOSCBal);
						}
					}
				}
				else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCR) ||
						pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCD)) {
					//A@P.01 Starts
					int rfnd_req_id = 0;
					rfnd_req_id = dbagrmt.readbyRfndRqstByAgrmtID(row.agrmt.agrmt_id);
					if(errorsPresent()) return null;
					
					if(rfnd_req_id >= JClaretyConstants.ZERO_INTEGER){
						BERfndReq beRfndReq = new BERfndReq();
						beRfndReq.rfnd_req_id = rfnd_req_id;
						beRfndReq = dbRfndReq.readByRfndReqId(beRfndReq);
						if(errorsPresent()) return null;
						
						if(beRfndReq != null && beRfndReq.rfnd_typ_cd != null){
							if(beRfndReq.rfnd_typ_cd.trim().equalsIgnoreCase(pBERfndReq.rfnd_typ_cd.trim())){
								addErrorMsg(1035, new String[] {"There is no balance to be refunded under the chosen type." });
								return null;
							}
						}
					}
					//A@P.01 Ends
//					fYAmt = this.getFscYrlyAllocInfo(row,pBEPln, fscYr); //1D@P.04
					
					fYAmt = this.getYearlyAllocInfo(row, pBEPln, calendar.get(Calendar.YEAR));//1A@P.04					
					if(errorsPresent()){
						return null;
					}					
					
					tmpBVOSCBal = new BVOSCBal();
					tmpBVOSCBal.bvagrmt = row;
					//put pending amount & posted amts seperately depending on status 
					//of agrmt, remember that inside GetFscYrlyAllocInfoRem we are 
					//accumulating just the post tax amt 
					if (row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_C) ||
							row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_R) ||
							row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MC)||
							row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_MR)) {
						tmpBVOSCBal.oscposttax = fYAmt;
					}
					else {
						tmpBVOSCBal.oscpndgposttax = fYAmt;
					}
					if ( tmpBVOSCBal != null) {
						tmpaoBVOSCBal.add(tmpBVOSCBal);
					}
				}
				else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBK)) {
					//A@P.01 Starts
					int rfnd_req_id = 0;
					rfnd_req_id = dbagrmt.readbyRfndRqstByAgrmtID(row.agrmt.agrmt_id);
					if(errorsPresent()) return null;
					
					if(rfnd_req_id >= JClaretyConstants.ZERO_INTEGER){
						BERfndReq beRfndReq = new BERfndReq();
						beRfndReq.rfnd_req_id = rfnd_req_id;
						beRfndReq = dbRfndReq.readByRfndReqId(beRfndReq);
						if(errorsPresent()) return null;
						
						if(beRfndReq != null && beRfndReq.rfnd_typ_cd != null){
							if(beRfndReq.rfnd_typ_cd.trim().equalsIgnoreCase(pBERfndReq.rfnd_typ_cd.trim())){
								addErrorMsg(1035, new String[] {"There is no balance to be refunded under the chosen type." });
								return null;
							}
						}
					}
					//A@P.01 Ends
					
					BVFndgRqmt bVFndgRqmt = new BVFndgRqmt();
					List aoBEAcctTrans;
					BigDecimal agrmtPSTDAmtPre = JClaretyConstants.ZERO_BIGDECIMAL;
					BigDecimal agrmtPSTDAmtPost = JClaretyConstants.ZERO_BIGDECIMAL;
					BigDecimal tmpAmtPre = null;
					BigDecimal tmpAmtPost = null;
					BigDecimal tmpAmtPreOSCInt = null;
					BigDecimal tmpAmtPostOSCInt = null;
					BigDecimal tmpAmtPreTrans = null;
					BigDecimal tmpAmtPostTrans = null;
					aoBEAcctTrans = dbaccttrans.readByAgrmt(row.agrmt);
					if(errorsPresent()){
						return null;
					}
					if ( aoBEAcctTrans != null) {
						tmpBVOSCBal = new BVOSCBal();
						tmpBVOSCBal.bvagrmt = row;
						int size1 = aoBEAcctTrans.size();
						for (int j = 0; j < size1; j++) {
							BEAcctTrans row1 = (BEAcctTrans) aoBEAcctTrans.get(j);
							bVFndgRqmt = dbfndgrqmt.sumByAcctTrans(row1);
							if(errorsPresent()){
								return null;
							}
							if ( bVFndgRqmt != null) {
								tmpAmtPre = bVFndgRqmt.pretaxamt;
								tmpAmtPost = bVFndgRqmt.posttaxamt;
								tmpAmtPreOSCInt = bVFndgRqmt.oscintpretaxamt;
								tmpAmtPostOSCInt = bVFndgRqmt.oscintposttaxamt;
								tmpAmtPreTrans = bVFndgRqmt.trans_pre_tax;
								tmpAmtPostTrans = bVFndgRqmt.trans_post_tax;
							}

							if (tmpAmtPre != null) {agrmtPSTDAmtPre = agrmtPSTDAmtPre.add(tmpAmtPre);
							}
							if (tmpAmtPost != null) {
								agrmtPSTDAmtPost = agrmtPSTDAmtPost.add(tmpAmtPost);
							}
							if (tmpAmtPreOSCInt != null) {
								agrmtPSTDAmtPre = agrmtPSTDAmtPre.add(tmpAmtPreOSCInt);
							}
							if (tmpAmtPostOSCInt != null) {
								agrmtPSTDAmtPost = agrmtPSTDAmtPost.add(tmpAmtPostOSCInt);
							}

						}
						tmpBVOSCBal.oscpndgpretax = row.agrmt.paid_pre_tax_amt.subtract(agrmtPSTDAmtPre);
						tmpBVOSCBal.oscpndgposttax = row.agrmt.paid_post_tax_amt.subtract(agrmtPSTDAmtPost);
						if (tmpBVOSCBal != null && (tmpBVOSCBal.oscpndgpretax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER || tmpBVOSCBal.oscpndgposttax
								.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER)) {
							tmpaoBVOSCBal.add(tmpBVOSCBal);//need to refund
						}
					}
				}else if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_EMR)) {//P.02	starts
					
					int rfnd_req_id = 0;
					rfnd_req_id = dbagrmt.readbyRfndRqstByAgrmtID(row.agrmt.agrmt_id);
					if(errorsPresent()) return null;
					
					if(rfnd_req_id >= JClaretyConstants.ZERO_INTEGER){
						BERfndReq beRfndReq = new BERfndReq();
						beRfndReq.rfnd_req_id = rfnd_req_id;
						beRfndReq = dbRfndReq.readByRfndReqId(beRfndReq);
						if(errorsPresent()) return null;
						
						if(beRfndReq != null && beRfndReq.rfnd_typ_cd != null){
							if(beRfndReq.rfnd_typ_cd.trim().equalsIgnoreCase(pBERfndReq.rfnd_typ_cd.trim())){
								addErrorMsg(1035, new String[] {"There is no balance to be refunded under the chosen type." });
								return null;
							}
						}
					}
					
					
					BVFndgRqmt bVFndgRqmt = new BVFndgRqmt();
					List aoBEAcctTrans;
					BigDecimal agrmtPSTDAmtPre = JClaretyConstants.ZERO_BIGDECIMAL;
					//BigDecimal agrmtPSTDAmtPost = JClaretyConstants.ZERO_BIGDECIMAL;
					BigDecimal tmpAmtPre = JClaretyConstants.ZERO_BIGDECIMAL;
					BigDecimal tmpAmtPost = null;
					
					aoBEAcctTrans = dbaccttrans.readByAgrmt(row.agrmt);
					if(errorsPresent()){
						return null;
					}
					BEFncDoc pFncDoc = new BEFncDoc();
					pFncDoc.fnc_doc_id = row.agrmt.fnc_doc_id;
					List aoFncItem = dbfncitem.readAllByFncDoc(pFncDoc);//read all contributions from fnc item
					if(errorsPresent()){
						return null;
					}
					if(aoFncItem != null){//1A@P.03
					for(int k =0; k<aoFncItem.size();k++){
						BEFncItem beFncItem = (BEFncItem)aoFncItem.get(k);
						if(beFncItem!=null && beFncItem.item_typ_cd.trim().equals("MPYR")){//add up only member contributions
							tmpAmtPre = tmpAmtPre.add(beFncItem.to_alloc_amt);
						}
					}
					if(errorsPresent()){
						return null;
					}
					}
					if ( aoBEAcctTrans != null) {
						tmpBVOSCBal = new BVOSCBal();
						tmpBVOSCBal.bvagrmt = row;
						int size1 = aoBEAcctTrans.size();
						for (int j = 0; j < size1; j++) {
							BEAcctTrans row1 = (BEAcctTrans) aoBEAcctTrans.get(j);
							bVFndgRqmt = dbfndgrqmt.sumByAcctTrans(row1);
							if(errorsPresent()){
								return null;
							}
							if ( bVFndgRqmt != null) {
								if( bVFndgRqmt.pretaxamt!=null){
								agrmtPSTDAmtPre = agrmtPSTDAmtPre.add(bVFndgRqmt.pretaxamt);
								}
							}
						}
						if(agrmtPSTDAmtPre.compareTo(tmpAmtPre)>JClaretyConstants.ZERO_INTEGER){
							tmpBVOSCBal.oscpndgpretax=agrmtPSTDAmtPre.subtract(tmpAmtPre);
						}else{
							tmpBVOSCBal.oscpndgpretax=tmpAmtPre.subtract(agrmtPSTDAmtPre);
						}
						
						if (tmpBVOSCBal != null && (tmpBVOSCBal.oscpndgpretax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER || tmpBVOSCBal.oscpndgposttax
								.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER)) {
							tmpaoBVOSCBal.add(tmpBVOSCBal);//need to refund
						}
					}
				}//P.02	 ends
			}
		}

		if (pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCR) ||
				pBERfndReq.rfnd_typ_cd.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCD)) {
			// Find out the base amount to check against in order to do IRC 415 refund
			// For IRC 415 refund, the FY to choose will be the pervious FY. So get the current FY from the application date
			// and figure the FY to use for disbursing IRC 415 money
			//*****This has been changed to use Calendar year vs Fiscal year for NPRIS-5570
			bVIRC415Lmts = this.getIRC415Amts(pBEPln,pEffDt, pBEMbrAcct);
			if(errorsPresent()){
				return null;
			}
			tmpFYAmt = JClaretyConstants.ZERO_BIGDECIMAL;
			boolean isPndg = false;
			if ( bVIRC415Lmts != null) {
				int size = tmpaoBVOSCBal.size();
				for (int i = 0; i < size; i++) {
					BVOSCBal row = (BVOSCBal) tmpaoBVOSCBal.get(i);
					if (bVIRC415Lmts.base_amt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {
						if (row.oscposttax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER) {
							isPndg = true;
							tmpFYAmt = row.oscpndgposttax;
						}
						else if (row.oscpndgposttax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) == JClaretyConstants.ZERO_INTEGER) {
							isPndg = false;
							tmpFYAmt = row.oscposttax;
						}
						if (bVIRC415Lmts.base_amt.compareTo(tmpFYAmt) < JClaretyConstants.ZERO_INTEGER) {
							//Update the o/p BV_OSC_Bal with the new base amt
							if (isPndg == true) {
								row.oscpndgposttax = tmpFYAmt.subtract(bVIRC415Lmts.base_amt);
							}
							else {
								row.oscposttax = tmpFYAmt.subtract(bVIRC415Lmts.base_amt);
							}
							bVIRC415Lmts.base_amt = JClaretyConstants.ZERO_BIGDECIMAL;
						}
						else {
							bVIRC415Lmts.base_amt = bVIRC415Lmts.base_amt.subtract(tmpFYAmt);
							//Update the o/p BV_OSC_Bal with zero, because these 
							//agrmts are within limit & shouldn't be refunded
							if (isPndg == true) {
								row.oscpndgposttax = JClaretyConstants.ZERO_BIGDECIMAL;
							}
							else {
								row.oscposttax = JClaretyConstants.ZERO_BIGDECIMAL;
							}
						}
					}
				}
			}
		}
		//set the TmpaoBVOSCBal to the top level o/p BV_OSC_Bal

		bVOSCBal.aobvoscbal = tmpaoBVOSCBal;
		if (tmpaoBVOSCBal != null && tmpaoBVOSCBal.size() != ZERO_INTEGER) {
			int size = bVOSCBal.aobvoscbal.size();
			for (int i = 0; i < size; i++) {
				BVOSCBal row = (BVOSCBal) bVOSCBal.aobvoscbal.get(i);
				bVOSCBal.oscpretax = bVOSCBal.oscpretax.add(row.oscpretax);
				bVOSCBal.oscposttax = bVOSCBal.oscposttax.add(row.oscposttax);
				bVOSCBal.oscpndgposttax = bVOSCBal.oscpndgposttax.add(row.oscpndgposttax);
				bVOSCBal.oscpndgpretax = bVOSCBal.oscpndgpretax.add(row.oscpndgpretax);
			}
		}


		addTraceMessage("getAgrmtRfndAmt", FABM_METHODEXIT);
		startBenchMark("getAgrmtRfndAmt", FABM_METHODEXIT);
		return bVOSCBal;
	}
	/*
	 * purpose:This method handles the OSC tranfer for AIRT & IRC415
	 */
	/*
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					djain       06/02/2008  Initial migration from FORTE
	 * P.01	  80305				kgupta		07/17/2009	added code to skip the void agrmt for being transfered,
	 *													as the status from void to transfered is not valid.
	 * P.02	  NPRIS-5952		vgp			09/23/2013	Modify retirement after airtime OSC validation 
	 *************************************************************************************/
	public BVcheckSrvcAtRtrmt checkSrvcAtRtrmt(
			BEMbrAcct beMbrAcct, BEPln bePln,
			Date pRtrmtDt, Date pEffDt, boolean pTrnsfrAIRT)
	throws ClaretyException {
		addTraceMessage("checkSrvcAtRtrmt",FABM_METHODENTRY);
		startBenchMark("checkSrvcAtRtrmt", FABM_METHODENTRY);
		List aoBVSrvCrdtCatgAmt = new ArrayList();
		BigDecimal OOSSrv = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal INSSrv = JClaretyConstants.ZERO_BIGDECIMAL;
		List tmpaoBVAgrmt = null;
		List aoBVAgrmt = null;
//		BEOSCRqst beoscRqst = null;	//1D@P.02
		IntervalData intervalData = new IntervalData();	//1A@P.02
		BEMbr beMbr = null;
		BVcheckSrvcAtRtrmt bVcheckSrvcAtRtrmt = new BVcheckSrvcAtRtrmt();
		List aoBVSrvcCrdt = null;
		BVSrvCrdtCatgAmt bvSrvCrdtCatgAmt = null;
		BigDecimal totSrvCrdt = JClaretyConstants.ZERO_BIGDECIMAL;
		FABMMbrAcctMaint fabmMbrAcctMaint = (FABMMbrAcctMaint) getFABMInstance(FABMMbrAcctMaint.class);
		//Read the total PSTD and RCLC service credit
		aoBVSrvcCrdt = fabmMbrAcctMaint.readSrvcCrdtForTotByTypeNStat(beMbrAcct,false, -1, -1, null, bePln, pEffDt,false, false);
		if (errorsPresent()) {
			return null;
		}
		boolean isFound = false;
		if (aoBVSrvcCrdt != null&& aoBVSrvcCrdt.size() != JClaretyConstants.ZERO_INTEGER) {
			for (int i = 0; i < aoBVSrvcCrdt.size(); i++) {
				BVSrvcCrdt row = (BVSrvcCrdt) aoBVSrvcCrdt.get(i);
				if (row.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_PSTD)
						|| row.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_RCLC)) {
					for (int j = 0; j < aoBVSrvCrdtCatgAmt.size(); j++) {
						BVSrvCrdtCatgAmt row1 = (BVSrvCrdtCatgAmt) aoBVSrvCrdtCatgAmt.get(j);
						if (row1.srv_cat_cd.trim().equals(row.srv_cat_cd.trim())) {
							row1.srv_eqvt_qty = row1.srv_eqvt_qty.add(row.srv_eqvt_qty);
							totSrvCrdt = totSrvCrdt.add(row.srv_eqvt_qty);
							isFound = true;
							break;
						}
					}
					if (!isFound) {
						bvSrvCrdtCatgAmt = new BVSrvCrdtCatgAmt();
						bvSrvCrdtCatgAmt.srv_cat_cd = row.srv_cat_cd.trim();
						bvSrvCrdtCatgAmt.srv_eqvt_qty = row.srv_eqvt_qty;
						bvSrvCrdtCatgAmt.stat_cd = row.stat_cd;
						totSrvCrdt = totSrvCrdt.add(row.srv_eqvt_qty);
						aoBVSrvCrdtCatgAmt.add(bvSrvCrdtCatgAmt);
					}
					isFound = false;
				}
			}
		}
		for (int i = 0; i < aoBVSrvCrdtCatgAmt.size(); i++) {
			//Figure out the total out of state service credit in the member's account
			BVSrvCrdtCatgAmt row = (BVSrvCrdtCatgAmt) aoBVSrvCrdtCatgAmt.get(i);
			if (row.srv_cat_cd.trim().equals(JClaretyConstants.SRV_CAT_CD_OOSC)) {
				OOSSrv = OOSSrv.add(row.srv_eqvt_qty);
			}//Figure out the total non-out of state service credit in the member's account
			else {
				INSSrv = INSSrv.add(row.srv_eqvt_qty);
			}
		}
		if (OOSSrv.compareTo(INSSrv) > JClaretyConstants.ZERO_INTEGER) {
			bVcheckSrvcAtRtrmt.pExcessOOS = OOSSrv.subtract(INSSrv);
		}
		beMbr = dbmbr.readByMbrAcct(beMbrAcct);
		if (errorsPresent()) {
			return null;
		}
		//Select ALL agreements by member account -> Returns array of BV_Agrmt (prorated, completed, in process defereed for IRC415)
		if (beMbr != null && bePln != null) {
			tmpaoBVAgrmt = dbagrmt.readByMbrNPlnNOrgNStatGrp(beMbr,bePln, null, "All", pEffDt);
			if (errorsPresent()) {
				return null;
			}
		}
		// Check for Open Agreement
		if (tmpaoBVAgrmt != null && tmpaoBVAgrmt.size() != JClaretyConstants.ZERO_INTEGER) {
			for (int i = 0; i < tmpaoBVAgrmt.size(); i++) {
				BVAgrmt row = (BVAgrmt) tmpaoBVAgrmt.get(i);
				if (row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_I)	|| 
						row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_P)	|| 
						row.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_A)) {
					addErrorMsg(1035,new String[]{"Cannot Process Refund-OSC agreement exists for the member account."});
					return null;
				}
			}
		}
		// Check for Air time Agreement
		if (tmpaoBVAgrmt != null) {
			aoBVAgrmt = new ArrayList();
			for (int i = tmpaoBVAgrmt.size() - 1; i >= 0; i--) {
				BVAgrmt bvAgrmt = (BVAgrmt) tmpaoBVAgrmt.get(i);		
				if (bvAgrmt.agrmt.srv_crdt_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)) {
					// 1A@P.01
					if(!bvAgrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_V)){
						aoBVAgrmt.add(bvAgrmt);
					}
				}
			}
		}
		BVOSCBal tmpBVOSCBal = new BVOSCBal();
		BVOSCBal airtOSCBal = new BVOSCBal();
		airtOSCBal.aobvoscbal = new ArrayList();
		if (aoBVAgrmt != null && aoBVAgrmt.size() != JClaretyConstants.ZERO_INTEGER) {
			intervalData.setUnit(0,12,0,0,0,0,0);	//2A@P.02
			Date dt12MothsBeforeRtrmtDt = DateUtility.subtract(pRtrmtDt,intervalData); 
			tmpBVOSCBal = this.readAgrmtBalByAcctNEffDtNCtgry(beMbrAcct, bePln,	JClaretyConstants.CTGRY_PSTD,pEffDt, aoBVAgrmt);
			for (int i = 0; i < tmpBVOSCBal.aobvoscbal.size(); i++) {
				BVOSCBal row = (BVOSCBal) tmpBVOSCBal.aobvoscbal.get(i);
				//Get OSC request by agreement
//				beoscRqst = dboscrqst.readbyOSCAgrmt(row.bvagrmt);	//5D@P.02
//				if (errorsPresent()) {
//					return null;
//				}
//				if (beoscRqst != null) {
					//At the time of retirement we want to check whether member has bought any air time service 
					//If air time agreement found, find out whether all the agreements are posted ot not.
					//If not, then problem. P_AirtPndg will carry the info out
					//if all the agreements are posted, then find out whether the member is retiring within the promised
					//time frame. If not, then the member should get refund of the agreement. P_AirtRfnd carries the info out.
				if (row.oscpndgpretax.equals(JClaretyConstants.ZERO_BIGDECIMAL)	&& row.oscpndgposttax.equals(JClaretyConstants.ZERO_BIGDECIMAL)
						&& (!row.oscposttax.equals(JClaretyConstants.ZERO_BIGDECIMAL) ||
								!row.oscpretax.equals(JClaretyConstants.ZERO_BIGDECIMAL))) {
					// Agreement is posted. So check again retirement date
//						if (beoscRqst.eff_rtrmt_dt.compareTo(pRtrmtDt) < JClaretyConstants.ZERO_INTEGER) {	//1D@P.02
					if (row.bvagrmt.agrmt.pymt_end_dt == null
							|| row.bvagrmt.agrmt.pymt_end_dt
									.compareTo(dt12MothsBeforeRtrmtDt) < JClaretyConstants.ZERO_INTEGER) {	//1A@P.02
						bVcheckSrvcAtRtrmt.pAirtRfnd = bVcheckSrvcAtRtrmt.pAirtRfnd.add(row.bvagrmt.agrmt.srv_crdt_qty);
					}
					else {
						airtOSCBal.aobvoscbal.add(row);
					}
				}
//				}	//1D@P.02
			}
			// If everything looks good then transfer Airtime service
			Date dt = pEffDt;
			if (pTrnsfrAIRT == true) {
				if (airtOSCBal.aobvoscbal != null && airtOSCBal.aobvoscbal.size() != JClaretyConstants.ZERO_INTEGER) {
					this.handleOSCTrnsf(bePln,JClaretyConstants.REG_RTRMT_TYP_CD,dt, dt, airtOSCBal);
				}
			}
		}
		addTraceMessage("checkSrvcAtRtrmt", FABM_METHODEXIT);
		startBenchMark("checkSrvcAtRtrmt", FABM_METHODEXIT);
		return bVcheckSrvcAtRtrmt;
	}
	/*
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					djain       06/02/2008  Initial migration from FORTE
	 * P.01                     vsenthil    11/13/2008  Modified code for Refunds Batch Job
	 * P.02		80305			kgupta		07/17/2009	Added check of if(errerpresent()) after a fabm call to 
	 * 													follow coding standard and to avoid null pointer error.
	 *************************************************************************************/
	public void handleOSCTrnsf(BEPln pbePln,
			String pRfndTyp, Date peffDt, Date pPstngDt,
			BVOSCBal pBVOSCBal) throws ClaretyException {
		addTraceMessage("handleOSCTrnsf", FABM_METHODENTRY);
		startBenchMark("handleOSCTrnsf", FABM_METHODENTRY);
		BigDecimal pstdSrvCrdtAmt;
		if (pRfndTyp.trim().equals(	JClaretyConstants.RFND_TYP_CD_AIRD)	||
				pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_AIRR)	||
				pRfndTyp.trim().equals(JClaretyConstants.REG_RTRMT_TYP_CD)) {
			for (int i = 0; i < pBVOSCBal.aobvoscbal.size(); i++) {
				BVOSCBal row = (BVOSCBal) pBVOSCBal.aobvoscbal.get(i);
				if (row.bvagrmt != null) {
					//Change status of agreement to Transferred & Call FABMOSC.SaveAgrmt
					row.bvagrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_T;
					//Save the agreement
					BVFabmOscSaveAgrmt bvFabmOscSaveAgrmt = this.saveAgrmt(pbePln, peffDt,	null, false, pPstngDt,row.bvagrmt, true,
							null, false, false);
					// 3A@P.02		
					if(errorsPresent()){
						return;
					}
					if(bvFabmOscSaveAgrmt != null){
						pstdSrvCrdtAmt = bvFabmOscSaveAgrmt.pPstdSrvCrdtAmt;
					}
				}
			}
		}
		else if (pRfndTyp.trim().equals(
				JClaretyConstants.RFND_TYP_CD_IRCD)	|| pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCR)
//2D@P.01				
//				|| pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBK)	|| 
//				pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBD)
				) {
			if ((pBVOSCBal.oscpndgposttax != null && 
					pBVOSCBal.oscpndgposttax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER)
					|| (pBVOSCBal.oscpndgpretax != null && 
							pBVOSCBal.oscpndgpretax.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER)) {
				int fscYr;
				BEFscYr beFscYr;
				BEMbr beMbr = null;
				//create the fnc item
				BEFncItem beFncItem = new BEFncItem();
				BEFncItem tmpBEFncItem = new BEFncItem();
				beFncItem.item_typ_cd = JClaretyConstants.ITEM_TYP_CD_IRCT;
				beFncItem.item_amt = pBVOSCBal.oscpndgposttax;
				beFncItem.item_amt = beFncItem.item_amt
				.add(pBVOSCBal.oscpndgpretax);
				BEDocStatHist bedocStatHist = new BEDocStatHist();//to save the status of the financial document
				BEFncDoc beFncDoc = new BEFncDoc();
				beFncDoc.doc_dt = peffDt;
				BEMbrAcct tmpBEMbrAcct = null;
				//Get the fiscal year details for the plan
				beFscYr = dbfscyr.readByPln(pbePln);
				if (errorsPresent()) {
					return;
				}
				if (beFscYr != null) {
					fscYr = beFscYr	.computeFiscalYearOfDate(peffDt);
					beFncDoc.doc_fsc_yr = fscYr;
				}
				beFncDoc.doc_typ_cd = JClaretyConstants.DOC_TYP_CD_IRCT;
				beFncDoc.pln_id = pbePln.pln_id;
				if (pBVOSCBal != null	&& pBVOSCBal.aobvoscbal != null&& pBVOSCBal.aobvoscbal.size() > JClaretyConstants.ZERO_INTEGER) {
					tmpBEMbrAcct = pBVOSCBal.aobvoscbal.get(0).bvagrmt.mbr_acct;

					beMbr = dbmbr.readByMbrAcct(tmpBEMbrAcct);
					if (errorsPresent()) {
						return;
					}
					if (beMbr != null) {
						beFncDoc.prsn_id = beMbr.prsn_id;
					}
				}
				bedocStatHist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_E;
				bedocStatHist.doc_stat_dt = peffDt;
				if (beFncDoc != null) {
					fabmgltrans.saveFncDoc(pbePln, beMbr,beFncDoc, bedocStatHist, null,peffDt, pPstngDt);
					if (errorsPresent()) {
						return;
					}
				}
				if (beFncDoc != null && beFncItem != null	&& tmpBEMbrAcct != null) {
					fabmgltrans.saveFncItem(beFncDoc,tmpBEMbrAcct, beFncItem,peffDt, pPstngDt);
					if (errorsPresent()) {
						return;
					}
				}
				List aoTOItems = new ArrayList();
				List aoFROMItems = new ArrayList();
				//create the ToItems for the allocation
				aoTOItems.add(beFncItem);
				for (int i = 0; i < pBVOSCBal.aobvoscbal.size(); i++) {
					BVOSCBal row = (BVOSCBal) pBVOSCBal.aobvoscbal.get(i);
					if (row.oscpndgposttax != null && row.oscpndgpretax != null) {
						for (int j = 0; j < row.bvagrmt.aobepymtinstr.size(); j++) {
							BEPymtInstr row1 = (BEPymtInstr) row.bvagrmt.aobepymtinstr.get(j);
							if ((pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_ERBK) ||
									pRfndTyp.trim().equals(	JClaretyConstants.RFND_TYP_CD_ERBD))
									|| ((pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCD) ||
											pRfndTyp.trim().equals(JClaretyConstants.RFND_TYP_CD_IRCR)) && row1.pre_tax_in == false)) {
								if (row1.fnc_item_id.intValue() != JClaretyConstants.ZERO_INTEGER) {
									if (tmpBEFncItem == null) {
										tmpBEFncItem = new BEFncItem();
									}
									tmpBEFncItem.fnc_item_id = row1.fnc_item_id.intValue();
									tmpBEFncItem = dbfncitem.readByFncItem(tmpBEFncItem);
									if (errorsPresent()) {
										return;
									}
									if (tmpBEFncItem != null) {
										aoFROMItems.add(tmpBEFncItem);
									}
								}
							}
						}
						//Allocate money from the agrmt to the late interest
						if (aoTOItems != null && aoFROMItems != null) {
							fabmgltrans.allocateItems(aoFROMItems,aoTOItems,row.oscpndgposttax.add(row.oscpndgpretax),peffDt,pPstngDt);
							if (errorsPresent()) {
								return;
							}
						}
						if (row.bvagrmt != null	&& (pRfndTyp.trim()	.equals(JClaretyConstants.RFND_TYP_CD_IRCD) || 
								pRfndTyp.trim()	.equals(JClaretyConstants.RFND_TYP_CD_IRCR))) {
							//If agreement is Air time completed or Air time prorated then go ahead and save agreement
							//with modified paid pre tax and paid post tax
							row.bvagrmt.agrmt.paid_post_tax_amt = row.bvagrmt.agrmt.paid_post_tax_amt.subtract(row.oscpndgposttax);
							if (row.bvagrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_AC) ||
									row.bvagrmt.agrmt_stat_cd.trim().equals(JClaretyConstants.AGRMT_STAT_AR)) {
								BEOSCRqst beoscRqst = dboscrqst.readbyOSCAgrmt(row.bvagrmt);
								if (errorsPresent()) {
									return;
								}
								if (beoscRqst != null) {
									dbagrmt.saveAgrmt(row.bvagrmt,beoscRqst,row.bvagrmt.mbr_acct.acct_id);
									if (errorsPresent()) {
										return;
									}
								}
							}
							else {
								//Change status of agreement to deferred for IRC415 & Call FABMOSC.SaveAgrmt
								row.bvagrmt.agrmt_stat_cd = JClaretyConstants.AGRMT_STAT_D;
								//Save the agreement
								BVFabmOscSaveAgrmt bvfabmOscSaveAgrmt = this.saveAgrmt(pbePln,peffDt,null,false,pPstngDt,row.bvagrmt,
										true, null,false,false);
								pstdSrvCrdtAmt = bvfabmOscSaveAgrmt.pPstdSrvCrdtAmt;
							}
						}
					}
				} //end for
			}
		}
		addTraceMessage("handleOSCTrnsf", FABM_METHODEXIT);
		startBenchMark("handleOSCTrnsf", FABM_METHODEXIT);
	}

	/**
	 * Purpose: 
	 *	This method checks all eligibility to apply or to buy service credit
	 *	If this method is called from different OSC Tabs and also From the WEB. If called from the WEB the
	 *	caller should set P_BEOSCRqst.web_ind to True otherwise always the indicator is set to False:
	 *	If called from Tab Service Credit need to pass the value for the following: BE_Mbr_Acct, BE_Pyrl,
	 *	BE_Mbr, P_BEEmptHist, BE_Acty
	 *	If called from Tab OSC Request  need to pass the value for the following:
	 *	BE_Mbr_Acct, BE_Pyrl, BE_Mbr, BE_OSC_Rqst, P_RfndPymtDt
	 *	If called from Tab Agreement  need to pass the value for the following:
	 *	BE_Mbr_Acct, BE_Pyrl, BE_Mbr, BE_OSC_Rqst, BV_Agrmt
	 *	
	 */
	/*
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * PreDev                   F2J         07/28/2008  Tool conversion from Nebraska Forte
	 * NE.01                    djain       07/31/2008  Initial Conversion
	 * NE.02					jmisra		10/20/2008	Code modified after review.
	 * P.01	  PIR-80236			jmisra		07/13/2009	Modified for PIR-80236
	 * P.02	  PIR-6110			VenkataM	06/30/2015	Added condition for AIR time service credit purchase
	 * P.03	  PIR-6110			VenkataM	07/09/2015	Added code for requirement 6 
	 *************************************************************************************/
	public List checkOSCElig( BEPln pBEPln, BEMbrAcct pBEMbrAcct, BEPyrl pBEPyrl, BEMbr pBEMbr, BEOSCRqst pBEOSCRqst, BEEmptHist pBEEmptHist, Date pRfndPymtDt, BVAgrmt pBVAgrmt,BEActy pBEActy) throws ClaretyException {
		addTraceMessage( "checkOSCElig", FABM_METHODENTRY );
		takeBenchMark("checkOSCElig", FABM_METHODENTRY);
        
		BVOSCEligCheckMsg tmpBVOSCEligCheckMsg ;
		List<BVOSCEligCheckMsg> paoBVOSCEligCheckMsg = new ArrayList<BVOSCEligCheckMsg>();
		Date currDt = new Date();
		String srvTyp = JClaretyConstants.CLARETY_EMPTY_FIELD;
		BigDecimal srvQty = JClaretyConstants.ZERO_BIGDECIMAL;
		if (pBEPyrl != null && pBEOSCRqst == null){
			srvTyp = pBEPyrl.srv_typ_cd;
			srvQty = pBEPyrl.srv_eqvt_qty;
		} else if (pBEPyrl == null && pBEOSCRqst != null) {
			srvTyp = pBEOSCRqst.srv_crdt_typ_cd;
			srvQty = pBEOSCRqst.purch_srv_crdt_qty;
		}
        
		Date tmpDt  = new Date();
		IntervalData yearsinterval = new IntervalData();
		IntervalData mnthinterval = new IntervalData();
		IntervalData daysinterval = new IntervalData();
		IntervalData tmpDaysInterval  = new IntervalData();
		daysinterval.setUnit(0,0,1,0,0,0,0);
		mnthinterval.setUnit(0,1,0,0,0,0,0);
		Date tmpLastDt  = new Date();
		Calendar tempCal = Calendar.getInstance();
		tempCal.setTime(getTimeMgr().getCurrentDateOnly());
		tmpDaysInterval.setUnit(0,0,tempCal.get(Calendar.DAY_OF_MONTH),0,0,0,0);
		int fsclYrNr;
		tmpLastDt = DateUtility.subtract(getTimeMgr().getCurrentDateOnly(), tmpDaysInterval);
		fsclYrNr = dbfscyr.calculateFiscalYearNr(pBEPln, tmpLastDt);
		if (errorsPresent()){
			return null;
		}
        
		//Check if member is working for a min of 60 hours in school plan
		if (srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAI) || srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAA)
				|| srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)){
			//NE.02	- Begin
			List<BECntrb> aoBECntrb  = null;
			//NE.02	- End
			BigDecimal totHrs = JClaretyConstants.ZERO_BIGDECIMAL;
			aoBECntrb = dbcntrb.readCntrbForFsclYr(pBEMbrAcct, fsclYrNr, tmpLastDt);
			if (errorsPresent()){
				return null;
			}
            
			if (aoBECntrb != null){
				for (BECntrb row:aoBECntrb){
					Calendar calStrtDt = Calendar.getInstance();
					Calendar calTmpLastDt = Calendar.getInstance();
					Calendar calEndDt = Calendar.getInstance();
					calStrtDt.setTime(row.strt_dt);
					calTmpLastDt.setTime(tmpLastDt);
					calEndDt.setTime(row.end_dt);
					if (calStrtDt.get(Calendar.MONTH)+1 == calTmpLastDt.get(Calendar.MONTH)+1 && 
							calEndDt.get(Calendar.MONTH)+1 == calTmpLastDt.get(Calendar.MONTH)+1){
						totHrs = totHrs.add(row.srv_crdt_hrs);
					}
				}

				if (totHrs.intValue() < JClaretyConstants.SIXTY_INTEGER){
					tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
					tmpBVOSCEligCheckMsg.error_typ = "Member works less than 60 hours.  ";
					tmpBVOSCEligCheckMsg.ovrd_in = true;
					paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
				}

			}
			else {
				if (aoBECntrb == null){
					if (totHrs.intValue() < JClaretyConstants.SIXTY_INTEGER){
						tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
						tmpBVOSCEligCheckMsg.error_typ = "Member works less than 60 hours.  ";
						tmpBVOSCEligCheckMsg.ovrd_in = true;
						paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
					}
				}
			}
		}
      
		//Check For LOA, Air time, Voluntary Refund, Mandatory Refund, if member is contributing
		if ((srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAI) || srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAA) || 
				srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT) || srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD))
				&& pBEOSCRqst != null && pBEOSCRqst.web_in == false){
			List<BEPyrl> aoBEPyrl = null;
			boolean hasMbrCntrbtd = false;
			aoBEPyrl = dbpyrl.readForLatestPstngByYear(pBEMbrAcct,fsclYrNr);
			if (errorsPresent()){
				return null;
			}

			//1A@P.01
			if(aoBEPyrl != null){
				for (BEPyrl row:aoBEPyrl){
					if (getDateCompare().isDayOnOrAfter(tmpLastDt, row.strt_dt)  || 
							getDateCompare().isDayOnOrBefore(tmpLastDt, row.end_dt)){
						hasMbrCntrbtd = true;
					}
				}
			}
         
			if (hasMbrCntrbtd == false){
				tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
				tmpBVOSCEligCheckMsg.error_typ = "Member is not contributing. ";
				if (srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)){
					tmpBVOSCEligCheckMsg.ovrd_in = false;
				}
				else {
					tmpBVOSCEligCheckMsg.ovrd_in = true;
				}

				paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
			}
		}
     
		//Check if LOA member has returned to work within 1 year after LOA expiration
		if (srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAI) || srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_LOAA)){
			List<BELOA> aoTempLOA  = new ArrayList<BELOA>();
			List<BELOA> aoBELOA = null;
			int j  = JClaretyConstants.ZERO_INTEGER;
			Date tempDate  = new Date();
			//Get all the leave records for the member
			aoBELOA = dBLOA.readByMbrAcct(pBEMbrAcct);
			if (errorsPresent()){
				return null;
			}

			if (aoBELOA != null){
				//aoTempLOA is going to be populated with blob LOA records. For every break we will add a new row to the array
				aoTempLOA.add(j, new BELOA());
				aoTempLOA.get(j).strt_dt = aoBELOA.get(0).strt_dt; // first element
				aoTempLOA.get(j).end_dt = aoBELOA.get(0).end_dt; // first element
				for (int i=1;i<aoBELOA.size();i++){
					if (getDateCompare().isDayOnOrAfter(aoBELOA.get(i-1).end_dt, aoBELOA.get(i).end_dt) && 
							getDateCompare().isDayOnOrBefore(aoBELOA.get(i-1).strt_dt, aoBELOA.get(i).strt_dt)){// fully overlapping LOA

						continue;
					} 
					else if (getDateCompare().isDayBefore(aoBELOA.get(i-1).strt_dt, aoBELOA.get(i).strt_dt) && 
							getDateCompare().isDayBefore(aoBELOA.get(i-1).end_dt, aoBELOA.get(i).end_dt) && 
							getDateCompare().isDayOnOrAfter(aoBELOA.get(i-1).end_dt, aoBELOA.get(i).strt_dt)) {// partially overlapping LOA, extend end date

						aoTempLOA.get(j).end_dt = aoBELOA.get(i).end_dt;
					} 
					else if (getDateCompare().isDayOnOrBefore(aoBELOA.get(i-1).end_dt, aoBELOA.get(i).strt_dt)) {

						tempDate = DateUtility.add(aoBELOA.get(i-1).end_dt, daysinterval);
						if (getDateCompare().isSameDay(tempDate,aoBELOA.get(i).strt_dt) || 
								getDateCompare().isSameDay(aoBELOA.get(i-1).end_dt, aoBELOA.get(i).strt_dt)){ // no break in LOA, extend end date
							aoTempLOA.get(j).end_dt = aoBELOA.get(i).end_dt;
						}
						else { // LOA break found
							j = j + 1;
							aoTempLOA.add(j, new BELOA());
							aoTempLOA.get(j).strt_dt = aoBELOA.get(i).strt_dt;
							aoTempLOA.get(j).end_dt = aoBELOA.get(i).end_dt;
						}
					}
				}

				// aoTempLOA  now contains blob of consecutive LOA information
				for (BELOA row:aoTempLOA){
					if (row.srv_typ != null && row.srv_typ.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MLSC)){
						continue;
					}

					yearsinterval.setUnit(JClaretyConstants.FOUR_INTEGER,0,0,0,0,0,0);
					tempDate = DateUtility.add(row.strt_dt, yearsinterval);
					if (getDateCompare().isDayAfter(row.end_dt, tempDate)){ // consecutive LOA applied for more than 4 years
						tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
						tmpBVOSCEligCheckMsg.error_typ = "Member cannot buy LOA. Consecutive LOA time is more than 4 years.  ";
						tmpBVOSCEligCheckMsg.ovrd_in = false;
						paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
					}
					else {
						BEPyrl bePyrl = dbpyrl.readForNextPstng(pBEMbrAcct.acct_id, row.end_dt);
						if (errorsPresent()){
							return null;
						}

						if (bePyrl != null){
							yearsinterval.setUnit(JClaretyConstants.ONE_INTEGER,0,0,0,0,0,0);
							tempDate = DateUtility.add(row.end_dt, yearsinterval);
							if (getDateCompare().isDayAfter(bePyrl.strt_dt, tempDate)){
								tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
								tmpBVOSCEligCheckMsg.error_typ = "Member has not returned to work within 1 year. ";
								tmpBVOSCEligCheckMsg.ovrd_in = false;
								paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
							}

						}

					}

				}

			}

		}

		//Vesting Credit member must be applied for within 30 days from the employment date in all the plans
		if (srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_VCS) && pBEOSCRqst != null && pBEOSCRqst.web_in == false){
			BEPrcs bePrcs = null;
			List<BVEmptTab> aoBVEmptTab = null;
			List<BVEmptTab> tmpaoBVEmptTab  = new ArrayList<BVEmptTab>();
			BEOrg beOrg = null;
			tmpDt = getDateFormatter().stringToDateMMDDYYYY(JClaretyConstants.CLARETY_MAX_DATE_mmddyyyy);
			bePrcs = dbPrcs.readByPrcsId(pBEActy.prcs_id);
			if (errorsPresent()){
				return null;
			}

			if (bePrcs != null){
				aoBVEmptTab = dbempthist.readOpenEmptForMbr(pBEMbr);
				if (errorsPresent()){
					return null;
				}

			}

			if (aoBVEmptTab != null){
				for (BVEmptTab row:aoBVEmptTab){
					if (row.idIsEqual(pBEEmptHist)){
						continue;
					} else if (row.bEPln.idIsEqual(pBEPln)) {

						continue;
					} else if (getDateCompare().isDayBefore(row.strt_dt, pBEEmptHist.end_dt)) {

						continue;
					} else if (getDateCompare().isDayAfter(row.strt_dt, pBEEmptHist.end_dt) && 
							getDateCompare().isDayBefore(row.end_dt, tmpDt)) {
                            
						//Get the type of the Org & check if it's an employer
						beOrg = dborg.readByOrgId(row.org_id);
						if (errorsPresent()){
						 	return null;
						}
                       
						if (beOrg != null && beOrg.org_typ_cd.trim().equals(JClaretyConstants.ORG_TYP_CD_EMPR)){
							tmpaoBVEmptTab.add(row);
						}
                        
					}
                    
				}

			}

			for (BVEmptTab row:tmpaoBVEmptTab){
				daysinterval.setUnit(30,0,0,0,0,0,0);
				tmpDt = DateUtility.add(row.strt_dt, daysinterval);
				if (getDateCompare().isDayAfter(tmpDt,bePrcs.prcs_strt_tm)){
					tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
					tmpBVOSCEligCheckMsg.error_typ = "Member is not Eligible. Vesting Credit must be applid within 30 days from the employment date. ";
					tmpBVOSCEligCheckMsg.ovrd_in = true;
					paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
					break;
				}

			}

		}

		//For air-time check if the member has at the maximum 12 months to retire from the current date
		if (srvTyp.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_AIRT)){
			if (pBEOSCRqst != null){
				if (pBEOSCRqst.rqst_typ.trim().equals(JClaretyConstants.RQST_TYP_CD_E)){
					//A member can get cost estimate 18 months ahead of retirement date
					mnthinterval.setUnit(0,18,0,0,0,0,0);
				}
				else {
					//For actual cost calculation member has to be within 12 months from retirement date
					mnthinterval.setUnit(0,12,0,0,0,0,0);
				}

				tmpDt = DateUtility.add(getTimeMgr().getCurrentDateOnly(), mnthinterval);
			}

			String tmpText ;
			currDt = getTimeMgr().getCurrentDateOnly();
			if (pBEOSCRqst != null && getDateCompare().isDayBefore(tmpDt, pBEOSCRqst.eff_rtrmt_dt)){
				tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
				if (pBEOSCRqst.rqst_typ.trim().equals(JClaretyConstants.RQST_TYP_CD_E)){
					tmpBVOSCEligCheckMsg.error_typ = "Member is not within 18 months of retirement. Not Eligible to purchase Air-time. ";
				}
				else {
					tmpBVOSCEligCheckMsg.error_typ = "Member is not within 12 months of retirement. Not Eligible to purchase Air-time. ";
				}

				tmpBVOSCEligCheckMsg.ovrd_in = false;
				paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
			}

			// @P.02 Starts
			BEPyrl bePyrl = dbpyrl
					.readFirstNonRvrsPstng(
							pBEMbrAcct,
							getValidateHelper()
									.getDateFormatter()
									.stringToDateMMDDYYYY(
											JClaretyConstants.CLARETY_MIN_DATE_mmddyyyy));
			Date date = getValidateHelper().getDateFormatter()
					.stringToDateMMDDYYYY(JClaretyConstants.AIR_TIME_DATE);
			if (getDateCompare().isDayOnOrAfter(bePyrl.strt_dt, date)) {

				BigDecimal totalPostSC = JClaretyConstants.ZERO_BIGDECIMAL;
				List<BVSrvcCrdt> aoBVSrvcCrdt = null;
				aoBVSrvcCrdt = fabmmbracctmaint.readSrvcCrdtForTotByTypeNStat(
						pBEMbrAcct, false, -1, -1, null, pBEPln, null, false,
						false);

				if (aoBVSrvcCrdt != null
						&& aoBVSrvcCrdt.size() > JClaretyConstants.ZERO_INTEGER) {
					for (int i = 0; i < aoBVSrvcCrdt.size(); i++) {
						BVSrvcCrdt row = (BVSrvcCrdt) aoBVSrvcCrdt.get(i);
						if (row.stat_cd.trim().equals(
								JClaretyConstants.CNTRB_STAT_CD_PSTD)) {
							totalPostSC = totalPostSC.add(row.srv_eqvt_qty);
						}
					}
				}
				if (errorsPresent()) {
					return null;
				}
				if (totalPostSC.compareTo(new BigDecimal(10.00)) < JClaretyConstants.ZERO_INTEGER) {
					tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
					tmpBVOSCEligCheckMsg.error_typ = "Minimum ten years of service credit must be credited to member's account ";
					tmpBVOSCEligCheckMsg.ovrd_in = false;
					paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);

				}
			}// @P.02 Ends
		}
		//For refund check if the member is applying for refund within 3 years after re-employment
		//and also that his break in service is not more than 5 years
		if (pBEOSCRqst != null){
			if (pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD) && 
				 	pBEOSCRqst.rqst_typ.trim().equals(JClaretyConstants.RQST_TYP_CD_F) && 
					(pBEPln.pln_id == JClaretyConstants.PLN_ID_ST || pBEPln.pln_id == JClaretyConstants.PLN_ID_CNTY)){
				List<BEEmptHist> aoBEEmptHist = null;
				BEEmptHist oldEmptHist= null;
				BEEmptHist newEmptHist = null;
				//Get all employments
				aoBEEmptHist = dbempthist.readAllEmptForMbrAcctByPlanId(pBEMbrAcct);
				if (errorsPresent()){
					return null;
				}

				if (aoBEEmptHist != null){
					for (BEEmptHist row:aoBEEmptHist){
						if (getDateCompare().isDayAfter(pRfndPymtDt, row.strt_dt)){ //means employment before refund
							oldEmptHist = row;
						}
						else {
							newEmptHist = row;
							break;
						}

					}

				}
                      
				//member should apply for refund within 3 years after getting re-employed... 
				//The re-employed employment is is NewEmptHist
				if (newEmptHist != null){
					yearsinterval.setUnit(3,0,0,0,0,0,0);
					tmpDt = DateUtility.add(newEmptHist.strt_dt, yearsinterval);
					if (getDateCompare().isDayBefore(tmpDt, pBEOSCRqst.appln_dt)){
						tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
						tmpBVOSCEligCheckMsg.error_typ = "3 years elapsed since refund, delay in applying for it. Member not eligible. ";
						tmpBVOSCEligCheckMsg.ovrd_in = false;
						paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
					}

					//check if his break in service is not more than 5 years
					yearsinterval.setUnit(5,0,0,0,0,0,0);
					tmpDt = DateUtility.add(oldEmptHist.end_dt, yearsinterval);
					if (getDateCompare().isDayBefore(tmpDt, newEmptHist.strt_dt)){
						tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
						tmpBVOSCEligCheckMsg.error_typ = "5 years elapsed before re-employment. Member not eligible.";
						tmpBVOSCEligCheckMsg.ovrd_in = false;
						paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
					}
				}
			}
			//@P.03 Starts
			if(pBEPln.pln_id == JClaretyConstants.PLN_ID_SCHL && pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD)){
				validateCostCalcPymntDates(pBEMbrAcct, pBEOSCRqst,null);
			}
			//@P.03 Ends
		}
		
		//Check if Srv_Crdt_Qty is within the max allowed
		BESrvCrdtRef beSrvCrdtRef = null;
		List<BVSrvcCrdt> aoBVSrvcCrdt = null ;
		beSrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNPln(srvTyp,pBEPln,null);
		if (errorsPresent()){
			return null;
		}
        
		if (beSrvCrdtRef != null){
			aoBVSrvcCrdt = fabmmbracctmaint.readSrvcCrdtForTotByTypeNStat(pBEMbrAcct,false,-1,-1,null, pBEPln,null, false, false);
			if (errorsPresent()){
				return null;
			}
            
			if (aoBVSrvcCrdt != null){
				for (BVSrvcCrdt row:aoBVSrvcCrdt){
					if (row.srv_typ_cd.trim().equals(srvTyp.trim()) && 
							(row.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_AGMT) || 
									row.stat_cd.trim().equals(JClaretyConstants.CNTRB_STAT_CD_PSTD))){
						srvQty = srvQty.add(row.srv_eqvt_qty);
					}
				}
			}
            
			if (srvQty.compareTo(beSrvCrdtRef.max_srv_crdt) > JClaretyConstants.ZERO_INTEGER){
				tmpBVOSCEligCheckMsg = new BVOSCEligCheckMsg();
				tmpBVOSCEligCheckMsg.error_typ = "Member's service exceeds the Max allowed limit of ";
				tmpBVOSCEligCheckMsg.error_typ = tmpBVOSCEligCheckMsg.error_typ.concat(beSrvCrdtRef.max_srv_crdt.toString());
				tmpBVOSCEligCheckMsg.error_typ = tmpBVOSCEligCheckMsg.error_typ.concat(" years. ");
				tmpBVOSCEligCheckMsg.ovrd_in = false;
				paoBVOSCEligCheckMsg.add(tmpBVOSCEligCheckMsg);
			}
            
		}
		addTraceMessage("checkOSCElig", FABM_METHODEXIT);
		takeBenchMark("checkOSCElig", FABM_METHODEXIT);
		return paoBVOSCEligCheckMsg;

	}
    
	/* ===================================================================================
	*        CCR#              Updated     Updated
	* Ver    PIR#              By          On          Description
	* ====   ==============    =========   ==========  ==================================
	* P.01   6110              VenkataM    08/21/2014   Added to validate the refund buy back payment date
	*************************************************************************************/
	public void validateCostCalcPymntDates(BEMbrAcct pBEMbrAcct, BEOSCRqst pBEOSCRqst, Date paymentEndDate) throws ClaretyException {
		addTraceMessage( "validateCostCalcPymntDates", FABM_METHODENTRY );
		takeBenchMark("validateCostCalcPymntDates", FABM_METHODENTRY);
        
		Date tmpDt  = new Date();
		IntervalData yearsinterval = new IntervalData();
		IntervalData mnthinterval = new IntervalData();
		//@P.03 Starts
		List<BEEmptHist> aoBEEmptHist = null;
		List<BEPyrl> aoBEPyrl = null;
		BEEmptHist oldEmptHist= null;
		BEEmptHist newEmptHist = null;
		Date dueTempDate = new Date();
		Date pyrlStartDate = null;
		IntervalData intervalData = new IntervalData();
		//Get all employments
		aoBEEmptHist = dbempthist.readAllEmptForMbrAcctByPlanId(pBEMbrAcct);
		
		IntervalData monthsDifference = new IntervalData();
		IntervalData oneDay = new IntervalData();
		oneDay.setUnit(0,0,1,0,0,0,0);
		Date reEmployedDate = getValidateHelper().getDateFormatter()
		.stringToDateMMDDYYYY(JClaretyConstants.EMPLYD_DATE);
		Date maxDate = getValidateHelper().getDateFormatter()
		.stringToDateMMDDYYYY(JClaretyConstants.CLARETY_MAX_DATE_mmddyyyy);
		Date rfndBuyBackEndDate = getValidateHelper().getDateFormatter()
		.stringToDateMMDDYYYY(JClaretyConstants.RFND_BUY_BACK_END_DATE);
		if (aoBEEmptHist != null){
			for (BEEmptHist row:aoBEEmptHist){
				if (getDateCompare().isDayOnOrAfter(row.strt_dt, reEmployedDate)){ //Checking if re-employed date is on or after 04/17/2014
					newEmptHist = row;
				}
				else if (getDateCompare().isDayBefore(row.strt_dt, reEmployedDate) && getDateCompare().isDayOnOrAfter(row.end_dt, maxDate)){ //Checking if member is active on 04/16/2014
					oldEmptHist = row;
				}
			}
		}
		
		if (newEmptHist != null){
			yearsinterval.setUnit(5,0,0,0,0,0,0);
			/*BEPyrl bePyrl = dbpyrl
			.readFirstNonRvrsPstng(
					pBEMbrAcct,newEmptHist.strt_dt);*///For the first bePyrl after the first employed or reemployed date
			BEPyrl bePyrl = dbpyrl
            .readFirstNonRvrsPstng(pBEMbrAcct,
            		DateUtility.subtract(newEmptHist.strt_dt, oneDay));
			
			//tmpDt = DateUtility.add(bePyrl.strt_dt, yearsinterval);//1M@P.02
			
			if (bePyrl != null) {
			      tmpDt = DateUtility.add(bePyrl.strt_dt, yearsinterval);//Calculating the date from 5 years from first re employment cntrb date
			} else {
			      tmpDt = DateUtility.add(newEmptHist.strt_dt, yearsinterval);//Calculating the date from 5 years from first re employment start date if cntrb not avaialable.
			}

			
			if(pBEOSCRqst != null){
			/*if (getDateCompare().isDayAfter(pBEOSCRqst.due_dt,tmpDt)){//checkng if application date is After the 5 years date.
				addErrorMsg(0, new String[]{" Due date should be before "+ getValidateHelper().getDateFormatter().dateStringMMDDYYYY(tmpDt)});
				return;
			}*/
			//Date currentDate = getTimeMgr().getCurrentDateOnly();
			if (getDateCompare().isDayAfter(pBEOSCRqst.due_dt,tmpDt)){//checkng if due date is After the 5 years date.
				addErrorMsg(0, new String[]{"Payment should be completed before "+ getValidateHelper().getDateFormatter().dateStringMMDDYYYY(tmpDt)});
			    return;
			}
			if(pBEOSCRqst.nmbr_of_pymts > JClaretyConstants.ZERO_INTEGER){
			mnthinterval.setUnit(0, pBEOSCRqst.nmbr_of_pymts, 0, 0, 0, 0, 0);
			dueTempDate = DateUtility.add(pBEOSCRqst.due_dt, mnthinterval);
			intervalData = intervalData.differenceInMonths(pBEOSCRqst.due_dt, tmpDt);
	        int monthdiff = intervalData.getUnit(IntervalData.DR_MONTH);

	        if (getDateCompare().isDayAfter(dueTempDate,tmpDt)){//checkng if due date according to the number of payments is After the 5 years date.
	        	addErrorMsg(0, new String[]{"Payment should be completed in less than "+monthdiff+ " payments"});
	        	return;
			}
			}
			}
			else if(paymentEndDate!=null){
				if (getDateCompare().isDayAfter(paymentEndDate,tmpDt)){//checkng if payment end date is After the 5 years date.
					addErrorMsg(0, new String[]{"Payment End Date should be before "+ getValidateHelper().getDateFormatter().dateStringMMDDYYYY(tmpDt)});
					return;
				}
			}
		}
		            
		if (oldEmptHist != null){
			if(pBEOSCRqst != null){
			/*if (getDateCompare().isDayAfter(pBEOSCRqst.due_dt,rfndBuyBackEndDate)){//checkng if due date is After the 5 years date.
				addErrorMsg(0, new String[]{"Due date should be before " + JClaretyConstants.RFND_BUY_BACK_END_DATE});
				return;
			}*/
			if (getDateCompare().isDayAfter(pBEOSCRqst.due_dt,rfndBuyBackEndDate)){//checkng if due date is After the 5 years date.
				addErrorMsg(0, new String[]{"Payment should be completed before " + JClaretyConstants.RFND_BUY_BACK_END_DATE});
				return;
			}
			if(pBEOSCRqst.nmbr_of_pymts > JClaretyConstants.ZERO_INTEGER){
			mnthinterval.setUnit(0, pBEOSCRqst.nmbr_of_pymts, 0, 0, 0, 0, 0);
			dueTempDate = DateUtility.add(pBEOSCRqst.due_dt, mnthinterval);
			intervalData = intervalData.differenceInMonths(pBEOSCRqst.due_dt, rfndBuyBackEndDate);
	        int monthdiff = intervalData.getUnit(IntervalData.DR_MONTH);
	        
			if (getDateCompare().isDayAfter(dueTempDate,rfndBuyBackEndDate)){//checkng if application date is After the 5 years date.
				addErrorMsg(0, new String[]{"Payment should be completed in less than "+monthdiff+ " payments"});
				return;
			}
			}
			}
			else if(paymentEndDate!=null){
				if (getDateCompare().isDayAfter(paymentEndDate,rfndBuyBackEndDate)){//checkng if payment end date is After the 5 years date.
					addErrorMsg(0, new String[]{"Payment End Date should be before " + JClaretyConstants.RFND_BUY_BACK_END_DATE});
					return;
				}
				
			}
		}
		
		//@P.03 Ends

	}
	/**
	 * Purpose: Get the refund amount without the refund amount for partial buyback cases.
	 */
	/*
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01                    vkundra     08/06/2008  Initial Conversion.
	 *************************************************************************************/
	public List getRfndAmt(BEMbrAcct pBEMbrAcct, List paoBVRfndTrans) throws ClaretyException {

		addTraceMessage("getRfndAmt", FABM_METHODENTRY);
		takeBenchMark("getRfndAmt", FABM_METHODENTRY);

		List aoBVPyrlDtl = null;	
		List aoIntAmt = null;
		List aoBVRfndTrans = null;

		//Populate the refund Panel
		if(pBEMbrAcct != null && aoBVRfndTrans == null){
			aoBVRfndTrans = dbaccttrans.readBVRfndTransByMbrAcct(pBEMbrAcct.acct_id);
			if(errorsPresent()){
				return null;
			}
		}
		//The refund amount and the interest amount must be added together to display on the Tab
		if(aoBVRfndTrans != null){
			for(int i=0; i<aoBVRfndTrans.size(); i++){
				BVRfndTrans row = (BVRfndTrans)aoBVRfndTrans.get(i);
				BEAcctTrans bEAcctTrans = new BEAcctTrans();
				bEAcctTrans.acct_trans_id = row.acct_trans_id;
				aoIntAmt = dbintitem.sumIntItemByRfndTrans(bEAcctTrans);
				if(errorsPresent()){
					return null;
				}
				if(aoIntAmt != null && aoIntAmt.size() > JClaretyConstants.ZERO_INTEGER){
					for(int j=0; j<aoIntAmt.size(); j++){
						BEIntItem intAmt = (BEIntItem)aoIntAmt.get(j);
						if(intAmt.item_amt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) != JClaretyConstants.ZERO_INTEGER){
							row.int_amt = row.int_amt.add(intAmt.item_amt);
							row.rfnd_amt = row.rfnd_amt.add(intAmt.item_amt);
						}
					}
				}
			}
		}
		if(paoBVRfndTrans == null){
			paoBVRfndTrans = aoBVRfndTrans;
		}else {
			for(int i=0; i<paoBVRfndTrans.size(); i++){
				BVRfndTrans row = (BVRfndTrans)paoBVRfndTrans.get(i);
				for(int j=0; j<aoBVRfndTrans.size(); j++){
					BVRfndTrans row1 = (BVRfndTrans)aoBVRfndTrans.get(j);
					if(row1.acct_trans_id == row.acct_trans_id){
						row.rfnd_amt = row1.rfnd_amt;
						break;
					}
				}
			}
		}

		addTraceMessage("getRfndAmt", FABM_METHODEXIT);
		takeBenchMark("getRfndAmt", FABM_METHODEXIT);
		return paoBVRfndTrans;
	}
	
	/**
	* Purpose: Picks up all the Initiated and Returned processes from workflow sub-system.
	* @return List
	*/					
	/*
	* Change History
	* ===================================================================================
	*        CCR#              Updated     Updated
	* Ver    PIR#              By          On          Description
	* ====   ==============    =========   ==========  ==================================
	* NE.01	         		   Lashmi      10/10/2008  Initial coding for NE
	* P.01	 PIR6134		   vgp		   05/13/2015  Removed all references to this method 
	* 												   due to OnBase performance issues.
	*************************************************************************************/

//	public List getAllInitiatedPrcs() throws ClaretyException
//	{
//		startBenchMark("getAllInitiatedPrcs", FABM_METHODENTRY);
//		addTraceMessage("getAllInitiatedPrcs", FABM_METHODENTRY);
//		
//		BVAllWorkList bvAllWorkList = new BVAllWorkList();
//		BVWorkList bvWorkList = null;
//		BEActyRef beActyRef = null;
//		DBActyRef dbActyRef = (DBActyRef) getDBInstance(DBActyRef.class);
//		FABMBusinessProcess fabmWorkflow = (FABMBusinessProcess) getFABMInstance(FABMBusinessProcess.class);
//		String sSortBy = null;
//		List aoBVAllWorkList = null;
//		List aoBVWorkList = new ArrayList();
//		
//		List aoActyList = null;
//		List aoActyList1 = null;
//		//Read for Process Regular Retirement 
//		aoActyList = (List)dbActyRef.readByFuncCdPrcsCd("RAP","RPRR");
//		if(errorsPresent())
//		{
//			return null;
//		}
//		//Read for Disability Retirement
//		aoActyList1 = (List)dbActyRef.readByFuncCdPrcsCd("RAP","RPDR");
//		if(errorsPresent())
//		{
//			return null;
//		}
//		if(aoActyList != null && aoActyList.size() > 0)
//		{
//			for(int i = 0; i < aoActyList.size() ; i++)
//			{		
//			   beActyRef = (BEActyRef)aoActyList.get(i);
//			   Assert.notNull("BEActyRef", beActyRef);
//			   bvAllWorkList.wrkf_acty_cd = beActyRef.wrkf_acty_cd;
//				bvAllWorkList.actyWorkCount = fabmWorkflow.wfGetWorkCount("",
//						FABMBusinessProcess.CW_VIEW_TYPE_WORKPOOL,
//						AppWorkflowMetadata.DATEINITIATED,
//						beActyRef.wrkf_acty_cd, beActyRef.prcs_cd);
//														
//				if (errorsPresent()) {
//					return null;
//				}												
//				//System.out.println("@@@@@@@@@@@ deepa size of bvAllWorkList.actyWorkCount?????"+bvAllWorkList.actyWorkCount);
//				if (sSortBy == null || sSortBy.compareTo("") == 0) {
//		  			sSortBy = AppWorkflowMetadata.DATEINITIATED;
//		  		}    			
//		  		//To get a list workflow activity instances
//		  		aoBVAllWorkList = (List) fabmWorkflow.wfGetWorkPool(beActyRef, sSortBy, bvAllWorkList.actyWorkCount);
//		  		
//		  		if (errorsPresent()) {
//					return null;
//				}
//				
//				if(aoBVAllWorkList != null)
//				{
//					for (int j= 0; j < aoBVAllWorkList.size(); j++) 
//					{ 
//		  			 bvWorkList = (BVWorkList) aoBVAllWorkList.get(j);
//		  			 Assert.notNull("BVWorkList", bvWorkList);
//		  			if (bvWorkList.sstatus.equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_INITIATED) 
//		  				|| bvWorkList.sstatus.equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_RETURNED)) 
//		  				{	
//		  				   System.out.println("bvWorkList.sstatus value " + bvWorkList.sstatus );
//		  				   System.out.println("@@@@@@@@@@@ deepa bvWorkList.prcs_cd in Fabmosc:"+bvWorkList.prcs_cd);
//		  					//if(bvWorkList.prcs_cd.equals(JClaretyConstants.WRKF_PRCS_CD_PUBI)){
//		  						aoBVWorkList.add(bvWorkList);		  						
//		  					}
//		  			   bvWorkList = null;
//		  				}
//					}
//				beActyRef = null;
//				}// end of for loop for aoActyList			
//			   
//			   
//			} //end of if aoActyList != null
//		
//		if(aoActyList1 != null && aoActyList1.size() > 0)
//		{
//			for(int i = 0; i < aoActyList1.size() ; i++)
//			{
//			   beActyRef = null;	
//			   beActyRef = (BEActyRef)aoActyList1.get(i);
//			   Assert.notNull("BEActyRef", beActyRef);
//			   bvAllWorkList = new BVAllWorkList();
//			   bvAllWorkList.wrkf_acty_cd = beActyRef.wrkf_acty_cd;
//				bvAllWorkList.actyWorkCount = fabmWorkflow.wfGetWorkCount("",
//						FABMBusinessProcess.CW_VIEW_TYPE_WORKPOOL,
//						AppWorkflowMetadata.DATEINITIATED,
//						beActyRef.wrkf_acty_cd, beActyRef.prcs_cd);
//														
//				if (errorsPresent()) {
//					return null;
//				}												
//				//System.out.println("@@@@@@@@@@@ deepa size of bvAllWorkList.actyWorkCount?????"+bvAllWorkList.actyWorkCount);
//				if (sSortBy == null || sSortBy.compareTo("") == 0) {
//		  			sSortBy = AppWorkflowMetadata.DATEINITIATED;
//		  		}    			
//		  		//To get a list workflow activity instances
//				aoBVAllWorkList = null;
//		  		aoBVAllWorkList = (List) fabmWorkflow.wfGetWorkPool(beActyRef, sSortBy, bvAllWorkList.actyWorkCount);
//		  		
//		  		if (errorsPresent()) {
//					return null;
//				}
//				
//				if(aoBVAllWorkList != null)
//				{
//					for (int j= 0; j < aoBVAllWorkList.size(); j++) 
//					{ 
//					 bvWorkList = null;	
//		  			 bvWorkList = (BVWorkList) aoBVAllWorkList.get(j);
//		  			 Assert.notNull("BVWorkList1", bvWorkList);
//		  			if (bvWorkList.sstatus.equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_INITIATED) 
//		  				|| bvWorkList.sstatus.equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_RETURNED)) 
//		  				{	
//		  				   System.out.println("bvWorkList.sstatus value " + bvWorkList.sstatus );
//		  				   System.out.println("@@@@@@@@@@@ deepa bvWorkList1.prcs_cd in Fabmosc:"+bvWorkList.prcs_cd);
//		  					//if(bvWorkList.prcs_cd.equals(JClaretyConstants.WRKF_PRCS_CD_PUBI)){
//		  						aoBVWorkList.add(bvWorkList);
//		  					}
//		  				}
//					}
//				}// end of for loop for aoActyList		
//			   
//			   
//			} //end of if aoActyList1 != null
//	
//		if(aoBVWorkList.size() == 0)
//		{ 
//			aoBVWorkList = null;
//		}
//		
//		
//		addTraceMessage("getAllInitiatedPrcs", FABM_METHODEXIT);
//		takeBenchMark("getAllInitiatedPrcs", FABM_METHODEXIT);
//		
//		return aoBVWorkList;
//		
//	}		
	/*********************************************************
	 * Method Name : CalculateFinanceCost()
	 *
	 * Input Parameters	:
	 *	P_BEMbrAcct : BE_Mbr_Acct
	 *	P_BEPln : BE_Pln
	 *	P_BEOSCRqst : BE_OSC_Rqst
	 *	P_BVAgrmt : BV_Agrmt
	 *	
	 * Output Parameters	:
	 *	P_BEOSCRqst : BE_OSC_Rqst
	 *	P_BVAgrmt : BV_Agrmt
	 *	P_FinancingCost : DecimalNullable
	 *
	 * Return Value :
	 *	ErrorStack
	 *
	 * Purpose :
	 *	This method calculates the financing cost.
	 *	Rule 1: If the call is from Request Tab  BV_Agrmt must be Nil
	 *	Rule 2: If the call is from Request Tab the output parameter P_FinancingCost will have the payment amount 
	 *	Rule 2: If the call is from Agrmt Tab BEOSCRqst must be Nil
	 *	Rule 3: The only time that this method expects more than one record in BVAgrmt.aoBEPymtInstr is 
	 *	when the service credit is:
	 *	1) Make up or DC Military. Map BVAgrmt.aoBEPymtInstr.pymt_amt with the total amount per payer id 
	 *	to this method, this amount will be overriden by the periodic payment within this method
	 *	2) For other types of service credit one payment instruction is expected and again pass the total payment 
	 *	amount in BVAgrmt.aoBEPymtInstr.pymt_amt  
	 *
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01                    djain       08/18/2008  Initial Conversion
	 * NE.02					jmisra		12/18/2008	Updated for fixing PIR-77783.
	 * NE.03                    djain       12/30/2008  modified for defects raised by lashmi
	 * P.01	 NPRIS-5945			vgp			08/29/2013	Do not calculate finance cost for mandatory refund.
	 *************************************************************************************/

	public BVCalculateFinanceCost calculateFinanceCost(BEMbrAcct pBEMbrAcct,
			BEPln pBEPln, BEOSCRqst pBEOSCRqst, BVAgrmt pBVAgrmt,
			String pCostCalcTypCd) throws ClaretyException {	//1M@P.01
		addTraceMessage("calculateFinanceCost", FABM_METHODENTRY);
		takeBenchMark("calculateFinanceCost", FABM_METHODENTRY);

		BEMbr beMbr;
		List<BVEmptTab> aoBVEmptTab;
		BEEmpr beEmpr;
		List<BEEmpr>  aoBEEmpr = new ArrayList<BEEmpr>();
		BigDecimal totCost = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal mnlyCost = JClaretyConstants.ZERO_BIGDECIMAL;
		boolean mnlyFlag = false;
		boolean smonFlag = false;
		boolean biwkFlag = false;
		boolean wklyFlag = false;
		int noofPymts = 0;
		BigDecimal tmpDcml = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal tmpDcml2 = JClaretyConstants.ZERO_BIGDECIMAL;
		BESrvCrdtRef beSrvCrdtRef;
		BVCalculateFinanceCost bvCalFinCost = new BVCalculateFinanceCost();


		if(pBEOSCRqst != null && pBVAgrmt == null){ //Tab OSC Request
			beSrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNMbrAcct(pBEMbrAcct,pBEOSCRqst.srv_crdt_typ_cd.trim(), null);
			if(errorsPresent()){
				return null;
			}
			if(beSrvCrdtRef != null){
				//Find the payroll frequency based on current employer if not already populated
				if(pBEOSCRqst.pyrl_freq == null || pBEOSCRqst.pyrl_freq.trim().equals(JClaretyConstants.CLARETY_EMPTY_FIELD) || 
						pBEOSCRqst.pyrl_freq.trim().equals(JClaretyConstants.CLARETY_SINGLE_BLANK_FIELD)){
					//Read member details
					beMbr = dbmbr.readByMbrAcct(pBEMbrAcct);
					if(errorsPresent()){
						return null;
					}
					if(beMbr == null || beMbr.mbr_id == JClaretyConstants.ZERO_INTEGER){
						addErrorMsg(56, new String[]{"Member","account"});
						return null;
					}

					//Read all open employments for the member
					aoBVEmptTab = dbempthist.readOpenEmptForMbr(beMbr);
					if(errorsPresent()){
						return null;
					}
					if(aoBVEmptTab != null){
						for(BVEmptTab row:aoBVEmptTab){
							//Check if this is an employer of the same plan as the member account
							if(row.bEPln.pln_id == pBEPln.pln_id){
								//Get employer details
								beEmpr = dbempr.readByOrgId(row.org_id);
								if(errorsPresent()){
									return null;
								}
								if(beEmpr != null){
									aoBEEmpr.add(beEmpr);
								}
							}
						}
					}
					if(aoBEEmpr != null && aoBEEmpr.size() > JClaretyConstants.ZERO_INTEGER){
						for(BEEmpr empr:aoBEEmpr){
							if(empr.wc_empr_rptg_freq_cd.trim().equals(JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_MNLY)){
								mnlyFlag = true;
								break;
							}
							else if(empr.wc_empr_rptg_freq_cd.trim().equals(JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_SMON)){
								smonFlag = true;
								break;
							}
							else if(empr.wc_empr_rptg_freq_cd.trim().equals(JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_BIWK)){
								biwkFlag = true;
								break;
							}
							else if(empr.wc_empr_rptg_freq_cd.trim().equals(JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_WKLY)){
								wklyFlag = true;
								break;
							}
						}

						//We want to set the no. of payments to the min. no. of payments available
						if(mnlyFlag == true){
							//Set payroll frequency to monthly - 12 payments
							pBEOSCRqst.pyrl_freq = JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_MNLY;
							if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
								pBEOSCRqst.nmbr_of_pymts = JClaretyConstants.NMBR_OF_PYMTS_MNLY_12;
							}
						}
						else if(smonFlag == true){
							//Set payroll frequency to semi-monthly - 24 payments
							pBEOSCRqst.pyrl_freq = JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_SMON;
							if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
								pBEOSCRqst.nmbr_of_pymts = JClaretyConstants.NMBR_OF_PYMTS_SMON_24;
							}
						}
						else if(biwkFlag == true){
							//Set payroll frequency to bi-weekly - 26 payments
							pBEOSCRqst.pyrl_freq = JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_BIWK;
							if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
								pBEOSCRqst.nmbr_of_pymts = JClaretyConstants.NMBR_OF_PYMTS_BIWK_26;
							}
						}
						else if(wklyFlag == true){
							//Set payroll frequency to weekly - 52 payments
							pBEOSCRqst.pyrl_freq = JClaretyConstants.WC_EMPR_RPTG_FREQ_CD_WKLY;
							if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
								pBEOSCRqst.nmbr_of_pymts = JClaretyConstants.NMBR_OF_PYMTS_WKLY_52;
							}
						}
					}
				}

				//If finance_in = TRUE it means we need to find amortization cost
				/*
				--	1M@P.02 Added MREF check. If it is a Mandatory Refund then no finance cost
				-- 	shd be computed.This method is called from winoscdetail and winpymtinstr
				--	When called from winoscdetail the cost_calc_typ = 'mref' which cud be used
				--	rather than this parameter. But, when called from winpymtinstr there is no
				--	way to figure out whether it is mref/vref. So made a unifrom change by adding
				--	a new parameter and passed the value from both the components(winoscdetail and
				--	winpymtinstr)
				 */
               //8D@NE.02  
				/*if(beSrvCrdtRef.finance_in == false || pSrvCrdtTypCd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MREF) || 
						(beSrvCrdtRef.finance_in == true && 
								(pBEOSCRqst.srv_crdt_typ_cd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_BRFD) && 
										(pBEOSCRqst.cost_calc_typ.trim().equals(JClaretyConstants.CALC_TYP_Interest) || 
												pBEOSCRqst.cost_calc_typ.trim().equals(JClaretyConstants.CALC_TYP_ROR))
								)		
						) 
				){*/
				if(beSrvCrdtRef.finance_in == false || pCostCalcTypCd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MREF)){	//1M@P.01
					totCost = pBEOSCRqst.base_cost;
					noofPymts = pBEOSCRqst.nmbr_of_pymts;

					if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
						bvCalFinCost.pFinancingCost = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					else{
						mnlyCost = totCost.divide(new BigDecimal(noofPymts), 4, BigDecimal.ROUND_HALF_UP);
						bvCalFinCost.pFinancingCost = mnlyCost;
					}
				}
				else{
					tmpDcml = pBEOSCRqst.base_cost;
					tmpDcml2 = new BigDecimal(pBEOSCRqst.nmbr_of_pymts);

					if(pBEOSCRqst.nmbr_of_pymts == JClaretyConstants.ZERO_INTEGER){
						bvCalFinCost.pFinancingCost = JClaretyConstants.ZERO_BIGDECIMAL;
					}
					else{
						//5D@NE.02
						//All other types of service credit
						/*bvCalFinCost.pFinancingCost = this.calculateAmortizedCost(tmpDcml,tmpDcml2,pBEOSCRqst.eff_dt,pBEPln,JClaretyConstants.INT_RATE_TYP_CD_AMRT);
						if(errorsPresent()){
							return null;
						}*/
						//10A@NE.02
						//All other types of service credit
						// Read the interest rate from interest rates table for the plan and request effective date
						BEIntRateRef beIntRateRef = dbIntRateRef.readByMaxEffDtNPlnIdNIntRateTypCd(pBEOSCRqst.eff_dt, pBEPln.pln_id, JClaretyConstants.INT_RATE_TYP_CD_AMRT.trim());
						if(errorsPresent()){
							return null;
						}
						bvCalFinCost.pFinancingCost = new BigDecimal(this.pmtCalculation(JClaretyConstants.TWELVE_INTEGER,pBEOSCRqst.nmbr_of_pymts, beIntRateRef.int_rate_pct.doubleValue(),tmpDcml.doubleValue()));
						if(errorsPresent()){
							return null;
						}
						//1D@NE.02
						//bvCalFinCost.pFinancingCost = bvCalFinCost.pFinancingCost.divide(new BigDecimal(pBEOSCRqst.nmbr_of_pymts), 4, BigDecimal.ROUND_HALF_UP);
					}
				}
			}
		}
		if(pBVAgrmt != null && pBEOSCRqst == null){
			BigDecimal agrmtTotCost = JClaretyConstants.ZERO_BIGDECIMAL;
			BigDecimal agrmtBaseCost = JClaretyConstants.ZERO_BIGDECIMAL;
			beSrvCrdtRef = dbsrvcrdtref.readBySrvCrdtTypNMbrAcct(pBEMbrAcct, pBVAgrmt.agrmt.srv_crdt_typ.trim(), null);
			if(errorsPresent()){
				return null;
			}
			if(beSrvCrdtRef != null){
				//If finance_in = TRUE it means we need to find amortization cost			
				if(beSrvCrdtRef.finance_in == false || pCostCalcTypCd.trim().equals(JClaretyConstants.SRV_CRDT_TYP_CD_MREF)){	//1M@P.01
					for(BEPymtInstr row:pBVAgrmt.aobepymtinstr){
						totCost = row.orig_amt;
						noofPymts = row.nbr_of_pymts;
						if(noofPymts > JClaretyConstants.ZERO_INTEGER){
							mnlyCost = totCost.divide(new BigDecimal(noofPymts), 4, BigDecimal.ROUND_HALF_UP);
							row.orig_amt = mnlyCost.multiply(new BigDecimal(noofPymts));
						}
						else{
							mnlyCost = JClaretyConstants.ZERO_BIGDECIMAL;
						}
						row.pymt_amt = mnlyCost;
						agrmtTotCost = agrmtTotCost.add(row.orig_amt);
					}
					agrmtBaseCost = agrmtTotCost;
				}
				else{
					for(BEPymtInstr row:pBVAgrmt.aobepymtinstr){
						noofPymts = row.nbr_of_pymts;
						//All other types of service credit
						tmpDcml = row.orig_amt;
						tmpDcml2 = new BigDecimal(row.nbr_of_pymts);
						if(tmpDcml2 != null && tmpDcml2.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
							//4D@NE.02
							/*bvCalFinCost.pFinancingCost = this.calculateAmortizedCost(tmpDcml,tmpDcml2,pBVAgrmt.agrmt.eff_dt,pBEPln,JClaretyConstants.INT_RATE_TYP_CD_AMRT);
							if(errorsPresent()){
								return null;
							}*/
							//10A@NE.02
							//All other types of service credit
							// Read the interest rate from interest rates table for the plan and request effective date
							BEIntRateRef beIntRateRef = dbIntRateRef.readByMaxEffDtNPlnIdNIntRateTypCd(pBVAgrmt.agrmt.eff_dt, pBEPln.pln_id, JClaretyConstants.INT_RATE_TYP_CD_AMRT.trim());//1M@NE.03
							if(errorsPresent()){
								return null;
							}
							bvCalFinCost.pFinancingCost = new BigDecimal(this.pmtCalculation(JClaretyConstants.TWELVE_INTEGER,row.nbr_of_pymts, beIntRateRef.int_rate_pct.doubleValue(),tmpDcml.doubleValue()));//1M@NE.03
							if(errorsPresent()){
								return null;
							}
							
							//mnlyCost = bvCalFinCost.pFinancingCost.divide(new BigDecimal(row.nbr_of_pymts), 4, BigDecimal.ROUND_HALF_UP);
							mnlyCost = bvCalFinCost.pFinancingCost.setScale(4, BigDecimal.ROUND_HALF_UP);//1A@NE.03
							row.pymt_amt = mnlyCost;
							agrmtTotCost = agrmtTotCost.add(row.pymt_amt.multiply(new BigDecimal(noofPymts)));
						}
						else{
							row.pymt_amt = JClaretyConstants.ZERO_BIGDECIMAL;
							agrmtTotCost = agrmtTotCost.add(row.orig_amt);
						}
					}
					agrmtBaseCost = pBVAgrmt.agrmt.base_cost;
					bvCalFinCost.pFinancingCost = null;
				}

				// Setting the tot_cost of agreement back
				pBVAgrmt.agrmt.tot_cost = agrmtTotCost;
				pBVAgrmt.agrmt.base_cost = agrmtBaseCost;
			}
		}
		bvCalFinCost.pBEOSCRqst = pBEOSCRqst;
		bvCalFinCost.pBVAgrmt = pBVAgrmt;
		addTraceMessage("calculateFinanceCost", FABM_METHODEXIT);
		takeBenchMark("calculateFinanceCost", FABM_METHODEXIT);
		return bvCalFinCost;
	}

	/*****************************************************************************************************
	 * Method Name : CalculateAmortizedCost()
	 *
	 * Input Parameters :
	 *
	 *	P_TotalCost : DecimalNullable
	 *	P_Period : DecimalNullable
	 *	P_EffDate : DateTimeData
	 *	P_BEPln : BE_Pln
	 *	P_IntRateTyp : String
	 *
	 * Output Parameters :
	 *	P_AmortCost : DecimalNullable
	 *
	 * Return Value :
	 *   ErrorStack
	 *
	 * Purpose :
	 *	Calculate the amortized cost
	 *   Assumptions : The total cost is amortized over 5 years
	 *				  The payment is made at the beginning of each year
	 *				  This code is to replicate the PMT function of MS Excel
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01                    djain       08/18/2008  Initial Conversion
	 * NE.02                    jmisra      10/20/2008  Code modified after revirew.
	 *************************************************************************************/
	public BigDecimal calculateAmortizedCost(BigDecimal pTotalCost,BigDecimal pPeriod,Date  pEffDate,
			BEPln pBEPln,String pIntRateTyp) throws ClaretyException{

		addTraceMessage("calculateAmortizedCost", FABM_METHODENTRY);
		takeBenchMark("calculateAmortizedCost", FABM_METHODENTRY);
		BigDecimal pAmortCost = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal basePymtCost = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal rngPrinCost = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal intPerPymt = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BigDecimal totalCost = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);
		BEIntRateRef beIntRateRef;
		BigDecimal intRate = JClaretyConstants.ZERO_BIGDECIMAL.setScale(4);

		// Read the interest rate from interest rates table for the plan and request effective date
		beIntRateRef = dbIntRateRef.readByMaxEffDtNPlnIdNIntRateTypCd(pEffDate, pBEPln.pln_id, pIntRateTyp.trim());
		if(errorsPresent()){
			return null;
		}
		if(beIntRateRef != null){
			intRate = beIntRateRef.int_rate_pct;
			// Divide intrate by the number of payments i,e, by P_Period
			intRate = beIntRateRef.int_rate_pct.divide(pPeriod, 4, BigDecimal.ROUND_HALF_UP);
			//Principal payoff per payment
			basePymtCost = pTotalCost.divide(pPeriod, 4, BigDecimal.ROUND_HALF_UP);
			// Need not pay any interest for the first payment if the payment is made at the
			// beginning of the period 
			totalCost = totalCost.add(basePymtCost);
			rngPrinCost = pTotalCost;
			// For 2nd to P_Period calculate payment value and add to the total cost
			//NE.02	- Begin
			for(int i=1;i<pPeriod.intValue();i++){
			//NE.02	- End
				rngPrinCost = rngPrinCost.subtract(basePymtCost);
				//Interest payment = Principal payment * rate of interest per payment
				intPerPymt = rngPrinCost.multiply(intRate);
				// total Yearly payoff = Principal payoff per year + interest 
				totalCost = totalCost.add(basePymtCost).add(intPerPymt);
			}
			pAmortCost = totalCost;
		}
		addTraceMessage("calculateAmortizedCost", FABM_METHODEXIT);
		takeBenchMark("calculateAmortizedCost", FABM_METHODEXIT);
		return pAmortCost;
	}
	
	/*
	 * Description : To calculate monthly payments over a rate of interest.	 
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     		Updated
	 * Ver    PIR#              By          		On          Description
	 * ====   ==============    =========   		==========  ==========================
	 * NE.01					jmisra	    		12/18/2008 	Newly Created to calculate PMT for fixing PIR-77783.			    
	 *************************************************************************************/ 
	public double pmtCalculation(int payments_per_year, int number_of_periods,
			double annual_interest_rate, double loan_amount){
		addTraceMessage("pmtCalculation", FABM_METHODENTRY);
		takeBenchMark("pmtCalculation", FABM_METHODENTRY);
		
		double payment_interest_rate = 1 / (1 + (annual_interest_rate/payments_per_year)); 
		payment_interest_rate = ((1 - payment_interest_rate) * loan_amount) / (payment_interest_rate * (1 - Math.pow(payment_interest_rate,number_of_periods))); 

		addTraceMessage("pmtCalculation", FABM_METHODEXIT);
		takeBenchMark("pmtCalculation", FABM_METHODEXIT);
		return payment_interest_rate;
	}
	
	/*
	 * Description : For any given paid amount and origianl loanPrinciapl, Calculate how much is 
	 * can be maximum interest.	 
	 * Change History
	 * ===================================================================================
	 * Ver	PIR#			Updated By	Updated	On	Description
	 * ==== ==============	==========  ==========  ==========================
	 * P.01	NPRIS-6297		vgp			04/01/2016	New method			    
	 *************************************************************************************/ 

	public BigDecimal calculateFinanceChargeForPaidAmount(
			BigDecimal pTotalPaidAmt, BigDecimal pIntialLoanPrincipal,
			BigDecimal pAnnualInterestRate, BigDecimal pMonthlyPaymentAmount) {
		addTraceMessage("calculateFinanceChargeFromPaidAmount",
				FABM_METHODENTRY);
		takeBenchMark("calculateFinanceChargeFromPaidAmount", FABM_METHODENTRY);

		double monthlyFinanceCharge = JClaretyConstants.ZERO_DOUBLE;
		double totFinanceCharge = JClaretyConstants.ZERO_DOUBLE;
		double totalPaidAmt = pTotalPaidAmt.doubleValue();
		double intialLoanPrincipal = pIntialLoanPrincipal.doubleValue();
		double annualInterestRate = pAnnualInterestRate.doubleValue();
		double monthlyPaymentAmount = pMonthlyPaymentAmount.doubleValue();

		// #installemnts paid will be determined by dividing amount paid by
		// installment amount
		int numberOfPaymentsMade = (int) (totalPaidAmt / monthlyPaymentAmount);
		// round it up if needed
		if ((monthlyPaymentAmount * numberOfPaymentsMade) < totalPaidAmt) {
			numberOfPaymentsMade = numberOfPaymentsMade + 1;
		}

		for (int month = 1; month <= numberOfPaymentsMade
				&& totalPaidAmt > JClaretyConstants.ZERO_DOUBLE; month++) {
			monthlyFinanceCharge = (annualInterestRate / JClaretyConstants.TWELVE_INTEGER)
					* intialLoanPrincipal;
			intialLoanPrincipal = intialLoanPrincipal
					- (monthlyPaymentAmount - monthlyFinanceCharge);
			if (totalPaidAmt >= monthlyPaymentAmount) {
				totFinanceCharge = totFinanceCharge + monthlyFinanceCharge;
				totalPaidAmt = totalPaidAmt - monthlyPaymentAmount;
			} else {
				monthlyFinanceCharge = monthlyFinanceCharge * totalPaidAmt
						/ monthlyPaymentAmount;
				totFinanceCharge = totFinanceCharge + monthlyFinanceCharge;
				totalPaidAmt = JClaretyConstants.ZERO_DOUBLE;
			}
		}

		addTraceMessage("calculateFinanceChargeFromPaidAmount", FABM_METHODEXIT);
		takeBenchMark("calculateFinanceChargeFromPaidAmount", FABM_METHODEXIT);
		return new BigDecimal(totFinanceCharge).setScale(2, BigDecimal.ROUND_HALF_UP);
	}
	
	/*********************************************************************************
	 * Method Name: createNAllocateRetSysPybl()
	 * 
	 * Input Parameters: 
	 *    P_BEPln : BE_Pln
	 *    P_BEEmpr : BE_Empr
	 *    P_BEWCRpt : BE_WC_Rpt
	 *    P_FscYrNr : Integer
	 *
	 * Output Parameters: 
	 *      None
	 *
	 * Return Value:
	 *      ErrorStack
	 *
	 * 	Purpose:
	 *		This method creates payables for retirement system for the amount passed in.
	 *       
	 *		
	 * Change History :
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01                    jmisra      08/20/2008  Initial Conversion
	 *************************************************************************************/
	public void createNAllocateRetSysPybl( BEPln pBEPln, List paoBVUnallocChks, Date pEffDt) throws ClaretyException {
		addTraceMessage("createNAllocateRetSysPybl", FABM_METHODENTRY);
		takeBenchMark("createNAllocateRetSysPybl", FABM_METHODENTRY);

		BigDecimal tmpamt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal pyblAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BERetSys beRetSys = null;
		List aoFncItem = new ArrayList();
		List aoTofncItem = new ArrayList();
		BEDocStatHist beDocStatHist = new BEDocStatHist();
		BEWCAcctPybl beWCAcctPybl = null;
		BVFABMGLTransSaveFncDoc bvFABMGLTransSaveFncDoc = null;
		BVFABMGLTransallocateItems bvFABMGLTransallocateItems = null;

		if(paoBVUnallocChks != null){
			for(int i = 0; i < paoBVUnallocChks.size(); i++ ){
				BVUnallocChks row = (BVUnallocChks)paoBVUnallocChks.get(i);
				if(row.ao_be_fnc_item != null){
					for(int j = 0; j < row.ao_be_fnc_item.size(); j++){
						BEFncItem item = (BEFncItem)row.ao_be_fnc_item.get(j);
						tmpamt =  item.item_amt.subtract(item.from_alloc_amt);
						pyblAmt = pyblAmt.add(tmpamt);
						if(tmpamt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER){
							aoFncItem.add(item);
						}
					}
				}
			}
		}
		if(pyblAmt != null && pyblAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER &&
				pyblAmt.compareTo(new BigDecimal(OSC_TOLERANCE_AMT)) <= JClaretyConstants.ZERO_INTEGER){
			//Read the retirement system details.
			beRetSys = dbRetSys.readForPlnId(pBEPln);
			if(errorsPresent()){
				return;
			}
			if(beRetSys != null){
				//Build Acct Pybl
				beWCAcctPybl = new BEWCAcctPybl();
				beWCAcctPybl.doc_dt = pEffDt;
				beWCAcctPybl.doc_fsc_yr = dbfscyr.calculateFiscalYearNr(pBEPln, pEffDt);
				if(errorsPresent()){
					return;
				}
				beWCAcctPybl.doc_typ_cd = JClaretyConstants.DOC_TYP_CD_AP;
				beDocStatHist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_E;
				beDocStatHist.doc_stat_dt = pEffDt;
				BEFncDoc beFncDoc = beWCAcctPybl;
				bvFABMGLTransSaveFncDoc = fabmgltrans.saveFncDoc(pBEPln, beRetSys, beFncDoc, beDocStatHist, null, pEffDt, pEffDt);
				if(errorsPresent()){
					return;
				}
				beFncDoc = bvFABMGLTransSaveFncDoc.befncdoc;
				beDocStatHist = bvFABMGLTransSaveFncDoc.bedocstathist;
				beWCAcctPybl = dbWCAcctPybl.save(null, beWCAcctPybl);
				if(errorsPresent()){
					return;
				}
				//Build the Financial Item
				if(pBEPln != null){
					BEFncItem beFncItem = new BEFncItem();
					beFncItem.fnc_doc_id = beWCAcctPybl.fnc_doc_id;
					beFncItem.from_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					beFncItem.to_alloc_amt = JClaretyConstants.ZERO_BIGDECIMAL;
					beFncItem.item_amt = pyblAmt;
					beFncItem.item_typ_cd = JClaretyConstants.ITEM_TYP_CD_AP;
					//Save the Financial Item
					beFncItem = fabmgltrans.saveFncItem(beWCAcctPybl, null, beFncItem, pEffDt, pEffDt);
					if(errorsPresent()){
						return;
					}
				}
				if(beWCAcctPybl != null){
					aoTofncItem = dbfncitem.readAllByFncDoc(beWCAcctPybl);
					if(errorsPresent()){
						return;
					}

				}
				if(aoTofncItem != null){
					bvFABMGLTransallocateItems = fabmgltrans.allocateItems(aoFncItem, aoTofncItem, null, pEffDt, pEffDt);
					if(errorsPresent()){
						return;
					}
					aoFncItem = bvFABMGLTransallocateItems.aoFromFncItems;
					aoTofncItem = bvFABMGLTransallocateItems.aoToFncItems;
					BEDocStatHist tmpBEDocStatHist = new BEDocStatHist();
					tmpBEDocStatHist.doc_stat_hist_opt_lck_ctl = JClaretyConstants.ZERO_INTEGER;
					tmpBEDocStatHist.doc_stat_hist_id = JClaretyConstants.ZERO_INTEGER;
					tmpBEDocStatHist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_A;
					tmpBEDocStatHist.doc_stat_dt = pEffDt;
					fabmwageandcntrb.calculateAndSetCRStat(tmpBEDocStatHist, paoBVUnallocChks);
					if(errorsPresent()){
						return;
					}
				}
			}
		}
		addTraceMessage("createNAllocateRetSysPybl", FABM_METHODEXIT);
		takeBenchMark("createNAllocateRetSysPybl", FABM_METHODEXIT);
		return;
	}

	/**
	 * Purpose: Calculate highest 12 month salary.
	 * @param BEMbrAcct
	 * @param BEPln
	 * @param Date
	 * @return BVFabmGBEPrepareAnlSalNCalaAvgComp
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On           Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.02					vkundra    09/03/2008  Generated for Tab Cost Calc.
	 * NE.02                    Lashmi     10/12/2008  Modified the code to do GrossUpCalculation
	 *                                                 and to cap Salary for RAP workflow accounts
	 *                                                 in School Plan
	 * P.01   PIR77742          Lashmi     12/12/2008  passed tmpLaoBEBeneEstmtWage instead of 
	 *                                                 paoBEBeneEstmtWage to prepareAnlSalary method  
	 * P.02	  NPRIS-5315		vgp		   10/03/2010  Fix infinite loop when pay prd end date is not end of month    
	 * P.03	  NPRIS-5430		vgp		   05/04/2012  Capping rule changes for 7% capping 
	 * P.04	  PIR6353			Venkat/vgp 07/26/2016  Patrol Tier2 Capping changes     
	 *************************************************************************************/

	public BVFabmGBEPrepareAnlSalNCalaAvgComp calculateHighest12mnthSal(BEMbrAcct pBEMbrAcct, BEPln pBEPln,
			Date pCostCalcDt)throws ClaretyException {

		addTraceMessage("calculateHighest12mnthSal", FABM_METHODENTRY);
		startBenchMark("calculateHighest12mnthSal", FABM_METHODENTRY);

		//This code is a code extract from FABMGBE.prepareBeneEstmtWage.The return object which relates to
		//GBE has been used to capture the blobERR.
		List pLaoBeneEstmtWage = new ArrayList();
		int  j=0;
		List paoBEBeneEstmtWage  = new ArrayList();
		List tmpLaoBEBeneEstmtWage = new ArrayList();
		List aoAnlSal = new ArrayList();
		Date payPeriodStartDate = new Date();
		Date payPeriodEndDate = new Date();
		BEBeneEstmtWage newBeBeneEstmtWage = null;
		int newYear = 0;
		int newMonth = 0;
		BVFabmGBEPrepareAnlSalNCalaAvgComp bvFabmGBEPrepareAnlSalNCalaAvgComp = new BVFabmGBEPrepareAnlSalNCalaAvgComp();
		Date firstDay = new Date();
		BEBeneEstmtWage  tempBeEstmtWage = new BEBeneEstmtWage();
		DBBeneEstmtWage dbBeneEstmtWage = (DBBeneEstmtWage)getDBInstance(DBBeneEstmtWage.class);

		paoBEBeneEstmtWage = dbBeneEstmtWage.readByAcctId(pBEMbrAcct, pCostCalcDt);
		if(errorsPresent()){
			return null;
		}
		
		if(paoBEBeneEstmtWage != null && paoBEBeneEstmtWage.size() > JClaretyConstants.ZERO_INTEGER){
			BEBeneEstmtWage beBeneEstmtWage = (BEBeneEstmtWage)paoBEBeneEstmtWage.get(0);
			Date newDate = new Date();
			Calendar calEndDt = Calendar.getInstance();
			calEndDt.setTime(beBeneEstmtWage.end_dt);

			newYear = calEndDt.get(Calendar.YEAR);
			newMonth = calEndDt.get(Calendar.MONTH)+1;
			newDate = tempBeEstmtWage.getLastDayOfMnth(newMonth, newYear);
			if(errorsPresent()){
				return null;
			}
			tempBeEstmtWage.end_dt = newDate;

			Calendar calStrDt = Calendar.getInstance();
			calStrDt.setTime(beBeneEstmtWage.strt_dt);

			int newStrYear = calStrDt.get(Calendar.YEAR);
			int newStrMonth = calStrDt.get(Calendar.MONTH)+1;
			int newStrDay = calStrDt.get(Calendar.DATE);
			String tdTmp = "";
			// If the pay period start date is not the first day of the month then make it the first day of the month
			if(newStrDay != JClaretyConstants.ONE_INTEGER){
				tdTmp = JClaretyConstants.CLARETY_EMPTY_FIELD;
				if(newStrMonth < JClaretyConstants.TEN_INTEGER){
					tdTmp = tdTmp.concat(JClaretyConstants.ZERO_INTEGER+"");
					tdTmp = tdTmp.concat(String.valueOf(newStrMonth));
					tdTmp = tdTmp.concat("/01/");
					tdTmp = tdTmp.concat(String.valueOf(newStrYear));
				}else{
					tdTmp = tdTmp.concat(String.valueOf(newStrMonth));
					tdTmp = tdTmp.concat("/01/");
					tdTmp = tdTmp.concat(String.valueOf(newStrYear));
				}
				firstDay = getValidateHelper().getDateFormatter().stringToDateMMDDYYYY(tdTmp);
				tempBeEstmtWage.strt_dt = firstDay;
			}else{
				tempBeEstmtWage.strt_dt = beBeneEstmtWage.strt_dt;
			} 
			tempBeEstmtWage.wag_amt = beBeneEstmtWage.wag_amt;
			tempBeEstmtWage.old_wag_amt = beBeneEstmtWage.old_wag_amt;
			if(beBeneEstmtWage.srv_crdt_hrs != null){
				tempBeEstmtWage.srv_crdt_hrs = beBeneEstmtWage.srv_crdt_hrs;
			} 
			tmpLaoBEBeneEstmtWage.add(tempBeEstmtWage);
			newYear = 0; 
			newMonth = 0;
			Date lastDay = new Date();
			IntervalData oneDay = new IntervalData();
			oneDay.setUnit(0,0,1,0,0,0,0);
			IntervalData oneMonth = new IntervalData();
			oneMonth.setUnit(0,1,0,0,0,0,0);
			if(paoBEBeneEstmtWage != null){
				for(j = 1; j < paoBEBeneEstmtWage.size(); j++){
					// If we have bi-weekly or semi-monthly payroll records they need to be
					// clubbed together
					beBeneEstmtWage = new BEBeneEstmtWage();
					beBeneEstmtWage = (BEBeneEstmtWage)paoBEBeneEstmtWage.get(j);
					BEBeneEstmtWage tempBEBeneEstmtWage = (BEBeneEstmtWage)tmpLaoBEBeneEstmtWage.get(tmpLaoBEBeneEstmtWage.size()-1);
					if((( beBeneEstmtWage.end_dt.after(tempBEBeneEstmtWage.strt_dt)
							||  beBeneEstmtWage.end_dt.equals(tempBEBeneEstmtWage.strt_dt))
							&&  (beBeneEstmtWage.strt_dt.before(tempBEBeneEstmtWage.end_dt)))){
						tempBEBeneEstmtWage.wag_amt = tempBEBeneEstmtWage.wag_amt.add (beBeneEstmtWage.wag_amt);
						tempBEBeneEstmtWage.srv_crdt_hrs = tempBEBeneEstmtWage.srv_crdt_hrs.add(beBeneEstmtWage.srv_crdt_hrs);
						continue;
					} 
					payPeriodEndDate = beBeneEstmtWage.end_dt;
					payPeriodStartDate = tempBEBeneEstmtWage.strt_dt;
					payPeriodStartDate = DateUtility.subtract(payPeriodStartDate, oneDay);
					Calendar calPayPrEndDt = Calendar.getInstance();
					calPayPrEndDt.setTime(payPeriodEndDate);
					Calendar calPayPrStrDt = Calendar.getInstance();
					calPayPrStrDt.setTime(payPeriodStartDate);
					while(!getDateCompare().isSameDay(payPeriodEndDate, payPeriodStartDate)){
						if(calPayPrEndDt.get(Calendar.MONTH) == calPayPrStrDt.get(Calendar.MONTH)&&
								calPayPrEndDt.get(Calendar.YEAR) == calPayPrStrDt.get(Calendar.YEAR)){
							newMonth = calPayPrEndDt.get(Calendar.MONTH)+1;
							newYear = calPayPrEndDt.get(Calendar.YEAR);
							lastDay = tempBeEstmtWage.getLastDayOfMnth(newMonth, newYear);
							if(errorsPresent()){
								return null;
							}
							beBeneEstmtWage.end_dt = lastDay;
							payPeriodEndDate = lastDay; 
							calPayPrEndDt.setTime(payPeriodEndDate);	//1A@P.02
						}else{
							newBeBeneEstmtWage = new BEBeneEstmtWage();
							newBeBeneEstmtWage.wag_amt = JClaretyConstants.ZERO_BIGDECIMAL;
							Date tmpStrtDt = new Date();
							//Date  tmpEndDt = new  Date  ()  ;
							tdTmp = JClaretyConstants.CLARETY_EMPTY_FIELD;
							tempBEBeneEstmtWage = (BEBeneEstmtWage)tmpLaoBEBeneEstmtWage.get(tmpLaoBEBeneEstmtWage.size()-1);
							tmpStrtDt = tempBEBeneEstmtWage.strt_dt;
							tmpStrtDt = DateUtility.subtract(tmpStrtDt, oneDay);
							Calendar calTmpStrtDt = Calendar.getInstance();
							calTmpStrtDt.setTime(tmpStrtDt);
							if(calTmpStrtDt.get(Calendar.MONTH)+1< JClaretyConstants.TEN_INTEGER){
								tdTmp = tdTmp.concat ( JClaretyConstants.ZERO_INTEGER +"")  ;
								tdTmp = tdTmp.concat(String.valueOf(calTmpStrtDt.get(Calendar.MONTH)+1));
								tdTmp = tdTmp.concat("/01/");
								tdTmp = tdTmp.concat (String.valueOf(calTmpStrtDt.get(Calendar.YEAR)));
							}else{
								tdTmp = tdTmp.concat(String.valueOf(calTmpStrtDt.get(Calendar.MONTH)+1));
								tdTmp = tdTmp.concat("/01/");
								tdTmp = tdTmp.concat (String.valueOf(calTmpStrtDt.get(Calendar.YEAR)));
							} 
							newBeBeneEstmtWage.end_dt = tmpStrtDt;
							newBeBeneEstmtWage.strt_dt = getValidateHelper().getDateFormatter().stringToDateMMDDYYYY(tdTmp);
							tmpLaoBEBeneEstmtWage.add(newBeBeneEstmtWage);
							payPeriodStartDate = DateUtility.subtract(getValidateHelper().getDateFormatter().stringToDateMMDDYYYY(tdTmp ), oneDay);
							calPayPrStrDt.setTime(payPeriodStartDate);	//1A@P.02
						} 
					} 
					// If the pay period start date is not the first day of the month then make it the first day of the month
					Calendar calEstmtStrtDt = Calendar.getInstance();
					calEstmtStrtDt.setTime(beBeneEstmtWage.strt_dt);
					if(calEstmtStrtDt.get(Calendar.DATE)!= JClaretyConstants.ONE_INTEGER){
						tdTmp = JClaretyConstants.CLARETY_EMPTY_FIELD;
						if(calEstmtStrtDt.get(Calendar.MONTH)+1< JClaretyConstants.TEN_INTEGER ){
							tdTmp = tdTmp.concat ( JClaretyConstants.ZERO_INTEGER +"");
							tdTmp = tdTmp.concat(String.valueOf(calEstmtStrtDt.get(Calendar.MONTH)+1));
							tdTmp = tdTmp.concat("/01/");
							tdTmp = tdTmp.concat (String.valueOf(calEstmtStrtDt.get(Calendar.YEAR)));
						}else{
							tdTmp = tdTmp.concat(String.valueOf(calEstmtStrtDt.get(Calendar.MONTH)+1));
							tdTmp = tdTmp.concat("/01/");
							tdTmp = tdTmp.concat(String.valueOf(calEstmtStrtDt.get(Calendar.YEAR)));
						} 
						firstDay = getValidateHelper().getDateFormatter().stringToDateMMDDYYYY(tdTmp);
						beBeneEstmtWage.strt_dt = firstDay;
					} 
					tmpLaoBEBeneEstmtWage.add(beBeneEstmtWage);
				}
				
				//A@P.04 Start
				FABMPlanTier fabmPlanTier =  (FABMPlanTier)getFABMInstance(FABMPlanTier.class);
				BVTierHistDtls bvTierHistDtls = fabmPlanTier.getCurrentPlanTierForMemberAcct(pBEMbrAcct);
				if (errorsPresent()) {
					return null;
				}
				if (bvTierHistDtls == null) {
					bvTierHistDtls = new BVTierHistDtls();
				}
				//A@P.04 End
				// If the plan is school then do the gross-up calculation
				if(pBEPln.pln_cli_cd.trim().equals(JClaretyConstants.PLN_CLI_CD_SCHOOL) && 
						tmpLaoBEBeneEstmtWage != null){
					FABMGBE fabmgbe = (FABMGBE)getFABMInstance(FABMGBE.class);
					tmpLaoBEBeneEstmtWage = fabmgbe.doGrossUpCalculation ( null, tmpLaoBEBeneEstmtWage);
					if(errorsPresent()){
						return null;
					}
				}	//1A@P.04
				if (RetirementPlanUtils.isSalaryCappingApplicable(
						pBEPln.pln_cli_cd.trim(),
						bvTierHistDtls.beTierRef.tier_ref_id)
						&& tmpLaoBEBeneEstmtWage != null) {	//1A@P.04
					boolean isRAP = this.checkForRAP(pBEMbrAcct);
					if(errorsPresent())
					{
						return null;
					}
					
					if(isRAP)
					{
						//A@P.03 Starts
						Date cappingEffStrtDt = pCostCalcDt;
						List aoLatestPyrl = dbpyrl.readForLatestPstng(pBEMbrAcct);
						if (aoLatestPyrl != null
								&& aoLatestPyrl.size() > JClaretyConstants.ZERO_INTEGER) {
							BEPyrl lclBePyrl = (BEPyrl) aoLatestPyrl.get(0);
							if (lclBePyrl != null
									&& lclBePyrl.end_dt != null) {
								cappingEffStrtDt = lclBePyrl.end_dt;
							}
						}
						//A@P.03 Ends
						tmpLaoBEBeneEstmtWage = this.checkExmptnsNCapSalaryForCube(pBEPln, tmpLaoBEBeneEstmtWage, pBEMbrAcct, cappingEffStrtDt);	//1A@P.03
						if(errorsPresent())
						{
							return null;
						}
					}
				}//end of if pBEPln.pln_cli_cd = School
				
				
				
				
				//1D@P.01
				//bvFabmGBEPrepareAnlSalNCalaAvgComp = this.prepareAnlSalary(pCostCalcDt,	pBEPln, paoBEBeneEstmtWage,aoAnlSal);
				//1A@P.01 - passed tmpLaoBEBeneEstmtWage instead of paoBEBeneEstmtWage to prepareAnlSalary method
				bvFabmGBEPrepareAnlSalNCalaAvgComp = this.prepareAnlSalary(pCostCalcDt,	pBEPln, tmpLaoBEBeneEstmtWage,aoAnlSal);
				if(errorsPresent()){
					return null;
				}
				Assert.notNull("bvFabmGBEPrepareAnlSalNCalaAvgComp", bvFabmGBEPrepareAnlSalNCalaAvgComp);
				aoAnlSal = bvFabmGBEPrepareAnlSalNCalaAvgComp.paoBEAnlSal;
				if(aoAnlSal != null && aoAnlSal.size() > JClaretyConstants.ZERO_INTEGER){
					aoAnlSal = QSortComparator.quickSort(aoAnlSal, "anl_sal_amt", "desc");
				}
				bvFabmGBEPrepareAnlSalNCalaAvgComp.paoBEAnlSal = aoAnlSal;
				
			}		
		}

		addTraceMessage("calculateHighest12mnthSal", FABM_METHODEXIT);
		takeBenchMark("calculateHighest12mnthSal", FABM_METHODEXIT);
		return bvFabmGBEPrepareAnlSalNCalaAvgComp;
	}
	
	/**********************************************************************************************
	 * Method Name		:	CheckExmptnsNCapSalaryForCube()
	 *
	 * Input Parameters 	:	
	 * 						P_BEPln:BE_Pln, 
	 *						P_LaoBEBeneEstmtWage : LargeArray of BE_Bene_Estmt_Wage	
	 *						P_BESrcMbrAcct : BE_Mbr_Acct 
	 *                      pCostCalcDe : Date
	 *
	 * Output Parameters :	
	 *						P_LaoBEBeneEstmtWage : LargeArray of BE_Bene_Estmt_Wage
	 *						
	 *
	 * Return Value		:	ErrorStack
	 *
	 * This method is an extraction from FABMGBE.checkExmptnsNCapSalary
	 *	 *
	 * Change History
	 * ==============================================================================================
	 *        CCR#               Updated   	Updated 
	 * Ver    PIR#               By        	On          Description
	 * =====  =================  ========= 	==========  ===========================================
	 * NE.01					 Lashmi		10/12/2008	Initial coding
	 * P.01	 NPRIS-5430			vgp			05/04/2012	Capping rule changes for 7% capping
	 * P.02	 NPRIS-5430			vgp			12/18/2012	Capping rule changes - Phase2  
	 * P.03	 NPRIS-5430			vgp			03/19/2013	Capping changes: Phase3
	 * P.04	 NPRIS-5897			vgp			06/18/2013	LB 236 - Cap salaries from multiple employers
	 * P.05	 PIR6353			vgp			07/26/2016	Patrol Tier2 Capping changes
	 *****************************************************************************************************/
	public List checkExmptnsNCapSalaryForCube(BEPln pBEPln,
			List pLaoBEBeneEstmtWage,
			BEMbrAcct pBESrcMbrAcct,
            Date pCostCalcDt ) throws ClaretyException{

		startBenchMark("checkExmptnsNCapSalaryForCube", FABM_METHODENTRY);
		addTraceMessage("checkExmptnsNCapSalaryForCube", FABM_METHODENTRY);
		
		
		BEMbr   bembr = new BEMbr();
		BEPrsn  beprsn= new  BEPrsn();
		List   tmpaoBEPyrl  = new ArrayList();
		List  aoBEPyrl = new ArrayList();
		List   aoBEEmpr  =new ArrayList();
		List  aoBEMbrExmptnDtls  = new ArrayList();

		BigDecimal  prevWagesSum = JClaretyConstants.ZERO_BIGDECIMAL;
		prevWagesSum.setScale(4);
		BigDecimal  currWagesSum = JClaretyConstants.ZERO_BIGDECIMAL;
		currWagesSum.setScale(4);
		Date   strtdt =new Date();
		Date   enddt =new Date();
		BigDecimal  wag_pct = JClaretyConstants.ZERO_BIGDECIMAL;
		wag_pct.setScale(4);
		int  fromFsclYr=0 ;
		int  toFsclYr=0  ;
		int  strtFsclYr=0 ;
		int  endFsclYr = 0;	//1A@P.01

		BigDecimal  increasedAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		increasedAmt .setScale(4);
		BigDecimal  capWagAmt= JClaretyConstants.ZERO_BIGDECIMAL;
		capWagAmt .setScale(6);
		BigDecimal  pyrlWagAmt= JClaretyConstants.ZERO_BIGDECIMAL;
		pyrlWagAmt .setScale(4);
		boolean  singleEmpt = false  ;
		boolean  salOverridden = false ;
		boolean  cappingToBEDone = true ;
		List  aoBEOvrdBeneEstmtWage  = new ArrayList();
		String  tmpDate = new  String();
		Date  startDate = new  Date() ;
		BESalCapRateRef beSalCapRateRef = null;		//2A@P.02
		DBSalCapRateRef dbSalCapRateRef = (DBSalCapRateRef)getDBInstance(DBSalCapRateRef.class);
		BESalCapRateRef beSalCapRateRefTier = null;	//3A@P.03
		BESalCapRateRef beSalCapRateRefTierPrev = null;
		BigDecimal  currWagesSumPrev = JClaretyConstants.ZERO_BIGDECIMAL.setScale(JClaretyConstants.FOUR_INTEGER);
        
		
	
		if(pBESrcMbrAcct !=null ){
			DBMbr dbMbr = (DBMbr)getDBInstance(DBMbr.class);
			bembr=dbMbr.readByMbrAcct ( pBESrcMbrAcct ) ;
			if(errorsPresent()){
				return null;
			}
		}else{
			addErrorMsg(209, new String[]{"Member Account"});
			return null;
		} 
	
		DBFscYr dbFscYr = (DBFscYr)getDBInstance(DBFscYr.class);  
		toFsclYr= dbFscYr.calculateFiscalYearNr (pBEPln, pCostCalcDt);
		if(errorsPresent()){
			return null;
		}
		
		//A@P.05 Start
		//get tier info
		FABMPlanTier fabmPlanTier =  (FABMPlanTier)getFABMInstance(FABMPlanTier.class);
		BVTierHistDtls bvTierHistDtls = fabmPlanTier.getCurrentPlanTierForMemberAcct(pBESrcMbrAcct);
		if (errorsPresent()) {
			return null;
		}
		if (bvTierHistDtls == null) {
			bvTierHistDtls = new BVTierHistDtls();
		}
		//A@P.05 End
		//A@P.02 Start
		//Get cap rate info
		beSalCapRateRef = dbSalCapRateRef.readSalCapRateByDatePlan(pBEPln.pln_id,pCostCalcDt, bvTierHistDtls.beTierRef.tier_ref_id);	//1M@P.05
		if (beSalCapRateRef == null || beSalCapRateRef.sal_cap_rate_pct == null) {
			cappingToBEDone = false;
		}
		//A@P.02 End
		//A@P.01 Start
		//Determine the date for capping end fscl year
		endFsclYr = toFsclYr;
		BVConstructFscDates lclBVConstructFscDates = dbFscYr.constructFscDates(pBEPln, toFsclYr, pCostCalcDt);
		if(errorsPresent()){
			return null;
		}
		if (getDateCompare().isDayBefore(pCostCalcDt, lclBVConstructFscDates.fscEndDt)){
			toFsclYr = toFsclYr - 1;
		}
		//A@P.01 End
			
	
		beprsn =  ( BEPrsn)( bembr);
		/*We are going to compare the past 5 years salary from CostCalcFiscal year. But
		Comparison has to happen from 2005 Fiscal year. ie., if the cost calc fiscal year is
		2011 then have 2006 as the base and compare 2006 + 5 years salary for capping.
		But if the rtrmt fiscal year happens to be 2007 then compare only 2 years salary
		taking 2005 as the base */

//		fromFsclYr = toFsclYr - JClaretyConstants.BaseFsclyr ; 	//1D@P.05
		fromFsclYr = toFsclYr
				- RetirementPlanUtils.getSalaryCappingDefaultBaseYear(
						pBEPln.pln_cli_cd.trim(),
						bvTierHistDtls.beTierRef.tier_ref_id);	//1A@P.05

		if(fromFsclYr > JClaretyConstants.FIVE_INTEGER ){
			fromFsclYr = toFsclYr - JClaretyConstants.FIVE_INTEGER;
		}else{
			fromFsclYr = toFsclYr - fromFsclYr ;
		}
		strtFsclYr = fromFsclYr ;		
		toFsclYr = endFsclYr;	//1A@P.01
		//Get the list of employers the member is currently working with. There are 
		//few records for which the empts are not ended. So select all those empts
		//who have reported after 2004
		DBEmpr dbEmpr = (DBEmpr)getDBInstance(DBEmpr.class);
		aoBEEmpr= dbEmpr.readEmprbyEmptNPyrl ( pBESrcMbrAcct, strtFsclYr );	//1M@P.02
		if(errorsPresent()){
			return null;
		}
	

		//Check if the amt is overriden from StrtFsclYr
	
		if(aoBEEmpr != null && aoBEEmpr.size() > JClaretyConstants.ZERO_INTEGER ){
			if( aoBEEmpr.size() == JClaretyConstants.ONE_INTEGER ){
				singleEmpt = true ;
			} 
		} 
		if(pLaoBEBeneEstmtWage != null && pLaoBEBeneEstmtWage .size() > JClaretyConstants.ZERO_INTEGER ){
			aoBEOvrdBeneEstmtWage = new ArrayList();
			tmpDate = "07/01/";
			tmpDate=tmpDate.concat(String.valueOf(strtFsclYr - 1 ))  ;
			startDate =getValidateHelper().getDateFormatter().stringToDateMMDDYYYY( tmpDate );
			BEBeneEstmtWage beneEstmtWage;
			if(pLaoBEBeneEstmtWage!= null){
				for (int i=0;i< pLaoBEBeneEstmtWage.size();i++ ) {
					beneEstmtWage = new BEBeneEstmtWage();
					beneEstmtWage=(BEBeneEstmtWage)pLaoBEBeneEstmtWage.get(i);      		 

					if(getDateCompare().isDayOnOrAfter(beneEstmtWage.strt_dt,startDate) &&
							( singleEmpt || beneEstmtWage.sal_ovrrd_in || beSalCapRateRef.empt_chng_cap_in)){	//1M@P.04 
						salOverridden = true ;
						aoBEOvrdBeneEstmtWage .add ( beneEstmtWage);
					}
				}
			}
		} 
	
		//IF Salary is overridden in case of multiple employers then
		//give an information to the users that Pioneer will not
		//determine if capping needs to be done for this case
		// Added P_IsMulEmprOverride check
		 
		if( ! singleEmpt && salOverridden && !beSalCapRateRef.empt_chng_cap_in){	//1M@P.04
			cappingToBEDone = false ;
		} 
		
		//Added CappingToBEDone check
		if(cappingToBEDone){
			if(aoBEEmpr != null && aoBEEmpr.size() > JClaretyConstants.ZERO_INTEGER ){
				BEEmpr  bEmpr=null;
				for (int j=beSalCapRateRef.empt_chng_cap_in? aoBEEmpr.size()-1 : 0;j< aoBEEmpr.size();j++ ) {	//1M@P.04
					// the fiscl year has to be reset
					bEmpr=(BEEmpr)aoBEEmpr.get(j);
					fromFsclYr = strtFsclYr ;
					//Construct the dates 
					BVConstructFscDates bvConstructFscDates = new BVConstructFscDates();
					bvConstructFscDates = dbFscYr.constructFscDates(pBEPln, fromFsclYr, null); 
					if(errorsPresent()){
						return null;
					}
					Assert.notNull("bvConstructFscDates", bvConstructFscDates);
					strtdt = bvConstructFscDates.fscStrtDt;
					enddt = bvConstructFscDates.fscEndDt;
					DBPyrl dbPyrl = (DBPyrl)getDBInstance(DBPyrl.class);
					//A@P.04 Start
					FABMGBE fabmgbe = (FABMGBE)getFABMInstance(FABMGBE.class);
					if (beSalCapRateRef.empt_chng_cap_in
							|| (singleEmpt && salOverridden)) {
						tmpaoBEPyrl = fabmgbe.checkNOverrideWages(
								aoBEOvrdBeneEstmtWage, strtdt, enddt, tmpaoBEPyrl);
						if (errorsPresent()) {
							return null;
						}
					} else {
						tmpaoBEPyrl = dbPyrl.readByEmprMbrAcctNStrDtEndDt(
								pBESrcMbrAcct, bEmpr, strtdt, enddt);
						if (errorsPresent()) {
							return null;
						}
					}

					for (int k = 0; tmpaoBEPyrl != null
					&& k < tmpaoBEPyrl.size(); k++) {
						prevWagesSum = prevWagesSum.add(((BEPyrl) tmpaoBEPyrl
								.get(k)).wag_amt);
					}
					//A@P.04 End
					//D@P.04 Start
//					tmpaoBEPyrl=  dbPyrl.readByEmprMbrAcctNStrDtEndDt(pBESrcMbrAcct, bEmpr, strtdt, enddt);
//					if(errorsPresent()){
//						return null;
//					}
//					FABMGBE fabmgbe = (FABMGBE)getFABMInstance(FABMGBE.class);
//					if(tmpaoBEPyrl != null && tmpaoBEPyrl.size() > JClaretyConstants.ZERO_INTEGER ){
//						if(singleEmpt && salOverridden ){ 
//							
//							tmpaoBEPyrl= fabmgbe.checkNOverrideWages(aoBEOvrdBeneEstmtWage,
//									strtdt, enddt, tmpaoBEPyrl); 
//							if(errorsPresent()){
//								return null;
//							}
//						} 
//						BEPyrl bePyrl=null;
//						for (int k=0;k< tmpaoBEPyrl.size();k++ ) {
//							bePyrl=(BEPyrl)tmpaoBEPyrl.get(k);        	
//							prevWagesSum = prevWagesSum.add ( bePyrl.wag_amt);
//						} 
//					} 
					//D@P.04 End
					
					while (fromFsclYr < toFsclYr) {
						//This will be set to true if capping is reqd. We need this
						//attribute to append the records to aoBEPyrl.
						boolean  capRqd = false ;
						//get the next fiscl yr sum to be compared
						fromFsclYr = fromFsclYr + 1 ;
						
						//Construct the dates
						bvConstructFscDates = new BVConstructFscDates();
						bvConstructFscDates = dbFscYr.constructFscDates(pBEPln, fromFsclYr, null);
						if(errorsPresent()){
							return null;
						}
						strtdt = bvConstructFscDates.fscStrtDt;
						enddt = bvConstructFscDates.fscEndDt;
						//A@P.04 Start
						if (beSalCapRateRef.empt_chng_cap_in
								|| (singleEmpt && salOverridden)) {
							tmpaoBEPyrl = fabmgbe.checkNOverrideWages(
									aoBEOvrdBeneEstmtWage, strtdt, enddt, tmpaoBEPyrl);
							if (errorsPresent()) {
								return null;
							}
						} else {
							tmpaoBEPyrl = dbPyrl.readByEmprMbrAcctNStrDtEndDt(
									pBESrcMbrAcct, bEmpr, strtdt, enddt);
							if (errorsPresent()) {
								return null;
							}
						}
						for (int k = 0; tmpaoBEPyrl != null
						&& k < tmpaoBEPyrl.size(); k++) {
							currWagesSum = currWagesSum
							.add(((BEPyrl) tmpaoBEPyrl.get(k)).wag_amt);
						}
						//A@P.04 End
						//D@P.04 Start
//						tmpaoBEPyrl= dbPyrl.readByEmprMbrAcctNStrDtEndDt(pBESrcMbrAcct, bEmpr, strtdt, enddt);
//						if(errorsPresent()){
//							return null;
//						}
//					
//						if(tmpaoBEPyrl != null && tmpaoBEPyrl .size() > JClaretyConstants.ZERO_INTEGER ){
//							if(singleEmpt && salOverridden ){ 
//								tmpaoBEPyrl= fabmgbe.checkNOverrideWages(aoBEOvrdBeneEstmtWage, strtdt, enddt, tmpaoBEPyrl);
//								if(errorsPresent()){
//									return null;
//								}
//							} 
//							BEPyrl bePyrl1=null;
//							if(tmpaoBEPyrl != null){
//								for (int i=0;i< tmpaoBEPyrl.size();i++ ) {
//									bePyrl1=(BEPyrl)tmpaoBEPyrl.get(i);
//									currWagesSum=currWagesSum.add(bePyrl1.wag_amt);       		 
//								}
//							} 
//						} 
						//D@P.04 End
						//A@P.03 Start - code for handling tiered capping 
						if (beSalCapRateRef.tier_in) {
							beSalCapRateRefTier = dbSalCapRateRef.readSalCapRateByDatePlan(pBEPln.pln_id,strtdt, bvTierHistDtls.beTierRef.tier_ref_id);	//1M@P.05	
							if (beSalCapRateRefTier == null || beSalCapRateRefTier.sal_cap_rate_pct == null) {
								beSalCapRateRefTier = beSalCapRateRef;
							} else {
								if (beSalCapRateRefTierPrev != null && beSalCapRateRefTier.tier_in != beSalCapRateRefTierPrev.tier_in) {
									prevWagesSum = currWagesSumPrev;
								}
							}
						} else {
							beSalCapRateRefTier = beSalCapRateRef;
						}	
						beSalCapRateRefTierPrev = beSalCapRateRefTier;
						currWagesSumPrev = currWagesSum;
						//A@P.03 End
						
						if( currWagesSum .compareTo(JClaretyConstants.ZERO_BIGDECIMAL )>JClaretyConstants.ZERO_INTEGER &&
								prevWagesSum.compareTo(JClaretyConstants.ZERO_BIGDECIMAL )>JClaretyConstants.ZERO_INTEGER ){ 
							//find if there is a 7%increase
							//wag_pct = new BigDecimal(JClaretyConstants.SEVEN_INTEGER).divide (new BigDecimal(JClaretyConstants.HUNDRED_INTEGER),2, ROUND_DECIMAL_DEFAULT);	//1D@P.02
							wag_pct = beSalCapRateRefTier.sal_cap_rate_pct.divide (new BigDecimal(JClaretyConstants.HUNDRED_INTEGER),6, ROUND_DECIMAL_DEFAULT);	//1A@P.02	//1M@P.03
							increasedAmt = prevWagesSum.multiply ( wag_pct).setScale(2,ROUND_DECIMAL_DEFAULT);	//1M@P.02
							increasedAmt= increasedAmt.add ( prevWagesSum);

							if(currWagesSum .compareTo(increasedAmt )>JClaretyConstants.ZERO_INTEGER){
								//CurrWages > IncreasedAmt
								//read exemptions 
								if (beSalCapRateRefTier.allw_exmptn_in && !beSalCapRateRef.empt_chng_cap_in) {	//1A@P.02	//1M@P.03	//1M@P.04
									DBMbrExmptnDtls dbMbrExmptnDtls = (DBMbrExmptnDtls)getDBInstance(DBMbrExmptnDtls.class);
									aoBEMbrExmptnDtls = dbMbrExmptnDtls.readByPrsnNOrgNYr ( beprsn,  bEmpr.org_cli_cd,
											fromFsclYr );
									if(errorsPresent()){
										return null;
									}
								} else {	//3A@P.02
									aoBEMbrExmptnDtls = null;
								}
								//If exemptions not exist capping has to be done
								if((aoBEMbrExmptnDtls == null) ||
										(aoBEMbrExmptnDtls != null && aoBEMbrExmptnDtls.size()==JClaretyConstants.ZERO_INTEGER)){ 
									capRqd = true ;    										
									/*The Current Wage Sum is over the allowed 7% increase,find the percent increase 
										of wage from the 7% increase and deduct that percent from each month. 
										For eg.If the year wage	is over the allowed 7% by an extra 2% then each month wage
										should be reduced by 2% */		
									capWagAmt = currWagesSum.subtract(increasedAmt);
									capWagAmt  = capWagAmt.divide(currWagesSum, 6, ROUND_DECIMAL_DEFAULT);
									currWagesSum = increasedAmt  ;
									BEPyrl bePyrl2=null;
									if(tmpaoBEPyrl!= null){
										for (int x=0;x<tmpaoBEPyrl.size();x++ ) {
											bePyrl2= new BEPyrl();
											bePyrl2=(BEPyrl)tmpaoBEPyrl.get(x);

											pyrlWagAmt = bePyrl2.wag_amt.multiply ( capWagAmt).setScale(3,ROUND_DECIMAL_DEFAULT);
											pyrlWagAmt = bePyrl2.wag_amt. subtract ( pyrlWagAmt);
											bePyrl2.wag_amt = pyrlWagAmt;

											/*There is a possibility that a person works under two diff emprs
											and salary needs to be capped for both of them. In such cases, if
											capping needs to be done for the same year, then do not append it to aoBEPyrl,
											compare the strt and end dates and add the wag amt. This is because the P_LaoBEBeneEstmtWage 
											will have only one record for a particualr strt dt and end dt combination and the last few stmts
											of the method will set only the amount corresponding to the last empr and not the sum of all the emprs
											(if this is not handled) */
											boolean  found = false ;
											BEPyrl bePyrl3=null;
											if(aoBEPyrl != null && aoBEPyrl.size() > JClaretyConstants.ZERO_INTEGER ){
												for (int k=0;k< aoBEPyrl.size();k++ ) {
													bePyrl3=(BEPyrl)aoBEPyrl.get(k);
													//Employer reporting do restirct the strt to be the first day of the month and 
													//end date to be the last day.
													
													if ((getDateCompare().isDayOnOrAfter(bePyrl3.strt_dt,bePyrl2.strt_dt)  &&
															getDateCompare().isDayOnOrBefore(bePyrl3.end_dt,bePyrl2.end_dt) )  ||
															(getDateCompare().isDayOnOrAfter(bePyrl2.strt_dt,bePyrl3.strt_dt)  &&
																	getDateCompare().isDayOnOrBefore(bePyrl2.end_dt,bePyrl3.end_dt) )){
														found = true ;
														bePyrl3.wag_amt = bePyrl3.wag_amt. add ( bePyrl2.wag_amt);
													} 
												} 
											} 
											if(!found ){ 
												aoBEPyrl.add ( bePyrl2);
											} 
										} 
									}
								} 
							} 
						} 
					

						BEPyrl tempBePyrl=null;
						BEPyrl finalPyrl=null;
						/* If the person has greater than 7% salary but has reported exemptions then
						 no caping is required. But still we might have to add it to aoBEPyrl.
						 Say a person works under 2 employers and the wage for fiscal year 2006 is
						 > (7% of wage for fiscal year 2005). And this person has exemptions reported
						under the second employer. The capping done for the first employer would already be
						present in aoBEPyrl. The wages of the second employer needs to be added so that the 
						total would be shown in Tab Sal/Bal. This is true even i)if the wages for the second employer
						does not require capping and ii)if the employer do not have prev wages*/
					
						if(! capRqd ){ 
							if(tmpaoBEPyrl!= null){
								for (int i=0;i< tmpaoBEPyrl.size();i++ ) {
									tempBePyrl = new BEPyrl();
									tempBePyrl=(BEPyrl)tmpaoBEPyrl.get(i);
									boolean  found = false ;
									if(aoBEPyrl != null && aoBEPyrl.size()> JClaretyConstants.ZERO_INTEGER ){
										if(aoBEPyrl!= null){
											for ( int x=0;x< aoBEPyrl.size();x++ ) {
												finalPyrl=(BEPyrl)aoBEPyrl.get(x);
												//Employer reporting do restirct the strt to be the first day of the month and 
												//end date to be the last day.
												
												if ((getDateCompare().isDayOnOrAfter(finalPyrl.strt_dt,tempBePyrl.strt_dt)  &&
														getDateCompare().isDayOnOrBefore(finalPyrl.end_dt,tempBePyrl.end_dt) )  ||
														(getDateCompare().isDayOnOrAfter(tempBePyrl.strt_dt,finalPyrl.strt_dt)  &&
																getDateCompare().isDayOnOrBefore(tempBePyrl.end_dt,finalPyrl.end_dt) )){

													found = true ;
													finalPyrl.wag_amt =finalPyrl.wag_amt.add (tempBePyrl.wag_amt);
												} 
											}
										}
									} 
									if(! found ){
										aoBEPyrl.add(tempBePyrl);
									} 
								} 
							}
						}
					
						//Reset the variables
						prevWagesSum= currWagesSum;
						capWagAmt =JClaretyConstants.ZERO_BIGDECIMAL;
						pyrlWagAmt =JClaretyConstants.ZERO_BIGDECIMAL;
						currWagesSum=JClaretyConstants.ZERO_BIGDECIMAL;
						if(errorsPresent()  ){
							break ;
						} 
					} 
				
					prevWagesSum =JClaretyConstants.ZERO_BIGDECIMAL;
					capWagAmt =JClaretyConstants.ZERO_BIGDECIMAL;
					pyrlWagAmt =JClaretyConstants.ZERO_BIGDECIMAL;
					currWagesSum =JClaretyConstants.ZERO_BIGDECIMAL;
					beSalCapRateRefTierPrev = null;	//3A@P.03
					beSalCapRateRefTier = null;
					currWagesSumPrev = JClaretyConstants.ZERO_BIGDECIMAL;
				} 
			}  
		} 
		if(cappingToBEDone ){//Added CappingToBEDone check 
			if(aoBEPyrl != null && aoBEPyrl .size() != JClaretyConstants.ZERO_INTEGER ){
				BEPyrl bPyrl=null;
				BEBeneEstmtWage bEstmtWage=null;
				for (int i=0;i<aoBEPyrl .size();i++ ) {
					//Modified this for P.03 as W&C does not restrict strt dt to be the first day and end dt to be the last day

					bPyrl=(BEPyrl)aoBEPyrl.get(i);
					if(pLaoBEBeneEstmtWage!= null){
						for (int j =0;j< pLaoBEBeneEstmtWage.size();j++) {
							bEstmtWage=(BEBeneEstmtWage)pLaoBEBeneEstmtWage.get(j);
							if ((getDateCompare().isDayOnOrAfter(bPyrl.strt_dt,bEstmtWage.strt_dt)  &&
									getDateCompare().isDayOnOrBefore(bPyrl.end_dt,bEstmtWage.end_dt) )  ||
									(getDateCompare().isDayOnOrAfter(bEstmtWage.strt_dt,bPyrl.strt_dt)  &&
											getDateCompare().isDayOnOrBefore(bEstmtWage.end_dt,bPyrl.end_dt) )){					

								//if single employment and if the wages are overridden then the same is taken care
								//in the above lines. Incase of multiple employment, if the wages are overriden then do nothing
								if(singleEmpt || (! singleEmpt && bEstmtWage. sal_ovrrd_in != true ) || beSalCapRateRef.empt_chng_cap_in){	//1M@P.04
									if ( bEstmtWage.wag_amt.compareTo( bPyrl.wag_amt)!= JClaretyConstants.ZERO_INTEGER ){
										bEstmtWage.cap_wag_amt = bEstmtWage.wag_amt;	//1A@P.03
										bEstmtWage.wag_amt = bPyrl.wag_amt;
										bEstmtWage.cap_in = true ;
									}else{ 
										bEstmtWage.wag_amt =bPyrl.wag_amt;
									} 
								}  
							} 
						} 
					}
				} 
			} 
		} 

		addTraceMessage("checkExmptnsNCapSalaryForCube", FABM_METHODEXIT);
		takeBenchMark("checkExmptnsNCapSalaryForCube", FABM_METHODEXIT);

		return pLaoBEBeneEstmtWage;
	}


	/**
	 * Purpose: Prepare salary.
	 * @param BEMbrAcct
	 * @param BEPln
	 * @param Date
	 * @return BVFabmGBEPrepareAnlSalNCalaAvgComp
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On           Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.01					vkundra    09/03/2008    Generated for Tab Cost Calc.
	 * NE.02					vkundra		10/24/2008		Comment Added
	 *************************************************************************************/
	public BVFabmGBEPrepareAnlSalNCalaAvgComp prepareAnlSalary(Date pCostCalcDt, BEPln pBEPln,
			List pLaoBEBeneEstmtWage, List paoBEAnlSal) throws ClaretyException {

		addTraceMessage("prepareAnlSalary", FABM_METHODENTRY);
		startBenchMark("prepareAnlSalary", FABM_METHODENTRY);

		int yearCount = 0;
		BigDecimal yearSum = JClaretyConstants.ZERO_BIGDECIMAL;
		Date endDt = new Date();
		endDt = ((BEBeneEstmtWage)pLaoBEBeneEstmtWage.get(0)).end_dt;
		int diffInMnths = 0;
		IntervalData oneDay = new IntervalData();
		oneDay.setUnit(0,0,1,0,0,0,0);
		BEAnlSal tmpBeAnlSal;
		String tdTempDateAsTxt = new String();
		paoBEAnlSal = new ArrayList();
		BECmnValdn tmpBECmnValdn = null;
		Date tmpDate = new Date();
		BigDecimal tmpVal = JClaretyConstants.ZERO_BIGDECIMAL;
		int termDateDay = 0;
		int termDateLastDay = 0;
		int dayDiff = 0;
		int fsclYr = 0;
		boolean pBlobErr = false;
		Calendar caltrmDt = Calendar.getInstance();
		BEPrsn becmnvaldn = new BEPrsn();
		int mnth = 0;

		caltrmDt.setTime(pCostCalcDt);
		mnth = caltrmDt.get(Calendar.MONTH)+1;
		int yrTrmDt = caltrmDt.get(Calendar.YEAR);
		int dayTrmDt = caltrmDt.get(Calendar.DATE);
		tmpDate = becmnvaldn.getLastDayOfMnth(mnth ,yrTrmDt);
		if(errorsPresent()){
			return null;
		}			
		termDateDay = dayTrmDt;

		Calendar caltempDt = Calendar.getInstance();
		caltempDt.setTime(tmpDate);
		int dayTempDt = caltempDt.get(Calendar.DATE);
		termDateLastDay = dayTempDt;

		if(pBEPln != null && pBEPln.pln_cli_cd.trim().equals(JClaretyConstants.PLN_CLI_CD_SCHOOL)){
			//PLEASE DO NOT DELETE THIS COMMENTED BLOCK
			//if the Rrtmt fiscal year is greater than 2005 then we have check if Exemptions exists or not then cap salary
			/*
			 //Get the Rrtmt fiscal year
			fsclYr = dbFscYr.calculateFiscalYearNr(pBEPln, pBEBeneEstmtParm.rtrmt_dt);
			if(errorsPresent()){
				return null;
			} 
			 * if(fsclYr > JClaretyConstants.BaseFsclyr ){
				pLaoBEBeneEstmtWage = this.checkExmptnsNCapSalary(pBEPln, pBEBeneEstmtParm, pLaoBEBeneEstmtWage, pIsMulEmprOverride, pBeSrcMbrAcct);
				if(errorsPresent()){
					return null;
				} 
			}*/
			if(pLaoBEBeneEstmtWage!= null){
				for(int row = 0; row < pLaoBEBeneEstmtWage.size(); row++){
					/*If the strt_dt and end_dt month and year does not match,we have a blob records*/
					BEBeneEstmtWage beBeneEstmtWage = (BEBeneEstmtWage)pLaoBEBeneEstmtWage.get(row);
					Calendar calStrDt = Calendar.getInstance();
					calStrDt.setTime(beBeneEstmtWage.strt_dt);
					int mnthStrDt = calStrDt.get(Calendar.MONTH)+1;
					int yrStrDt = calStrDt.get(Calendar.YEAR);
					Calendar calEndDt = Calendar.getInstance();
					calEndDt.setTime(beBeneEstmtWage.end_dt);
					int mnthEndDt = calEndDt.get(Calendar.MONTH)+1;
					int yrEndDt = calEndDt.get(Calendar.YEAR);
					IntervalData iD=new IntervalData();
					if((mnthStrDt!= mnthEndDt) || (yrStrDt!= yrEndDt)){
						iD.differenceInMonths(beBeneEstmtWage.end_dt, beBeneEstmtWage.strt_dt);
						diffInMnths = iD.getUnit(iD.DR_MONTH);
						yearCount = yearCount + diffInMnths + 1;
						yearSum = yearSum.add(beBeneEstmtWage.wag_amt);
						pBlobErr = true;
						if(yearCount > 12){
							endDt = DateUtility.subtract(beBeneEstmtWage.strt_dt, oneDay);
							yearCount = 0;
							yearSum = JClaretyConstants.ZERO_BIGDECIMAL;
							continue ;
						} 
					}else{
						yearCount = yearCount + 1;
						yearSum = yearSum.add(beBeneEstmtWage.wag_amt);
					} 
					if(yearCount == 12){
						/* If the yearcount is 12,prepare a be_anl_sal record and append it to the array of be_anl_sal*/
						tmpBeAnlSal = new BEAnlSal();
						tmpBeAnlSal.strt_dt = beBeneEstmtWage.strt_dt;
						tmpBeAnlSal.end_dt = endDt;
						tmpBeAnlSal.anl_sal_amt = yearSum;
						paoBEAnlSal.add(tmpBeAnlSal);
						endDt = DateUtility.subtract(tmpBeAnlSal.strt_dt, oneDay);
						yearCount = 0;
						yearSum = JClaretyConstants.ZERO_BIGDECIMAL;
					} 
				}
			}
		}else{
			if(pLaoBEBeneEstmtWage != null){
				for(int row = 0; row < pLaoBEBeneEstmtWage.size(); row++) {
					BEBeneEstmtWage beBeneEstmtWage = (BEBeneEstmtWage) pLaoBEBeneEstmtWage.get(row);
					Calendar calStrDt = Calendar.getInstance();
					calStrDt.setTime(beBeneEstmtWage.strt_dt);
					int mnthStrDt = calStrDt.get(Calendar.MONTH)+1;
					int yrStrDt = calStrDt.get(Calendar.YEAR);
					int dayStrDt = calStrDt.get(Calendar.DATE);
					Calendar calEndDt = Calendar.getInstance();
					calEndDt.setTime(beBeneEstmtWage.end_dt);
					int mnthEndDt = calEndDt.get(Calendar.MONTH)+1;
					int yrEndDt = calEndDt.get(Calendar.YEAR);
					int dayEndDt = calEndDt.get(Calendar.DATE);
					/*If the strt_dt and end_dt month and year does not match , we have a blob records*/
					if((mnthStrDt != mnthEndDt)||(yrStrDt != yrEndDt)){
						IntervalData iD = new IntervalData();
						iD = iD.differenceInMonths(beBeneEstmtWage.end_dt, beBeneEstmtWage.strt_dt);
						diffInMnths = iD.getUnit(iD.DR_MONTH);
						yearCount = yearCount + diffInMnths + 1;
						yearSum = yearSum.add(beBeneEstmtWage.wag_amt);
						pBlobErr = true;
						if((termDateDay != termDateLastDay && yearCount > 13)||
								(termDateDay == termDateLastDay && yearCount > 12)){
							endDt = DateUtility.subtract(beBeneEstmtWage.strt_dt,oneDay);
							yearCount = 0;
							yearSum = JClaretyConstants.ZERO_BIGDECIMAL; 
							continue;
						} 
					}else{
						yearCount = yearCount + 1;
						/*If the Cost Calculation date  day <> Last day of month and YearCount = 13*/
						if(termDateDay != termDateLastDay && yearCount == 13){
							dayDiff = dayEndDt - termDateDay;
							tmpVal = beBeneEstmtWage.wag_amt;
							tmpVal = tmpVal.multiply(new BigDecimal(dayDiff));
							tmpDate = becmnvaldn.getLastDayOfMnth(mnthEndDt, yrEndDt);
							if(errorsPresent()){
								return null;
							}
							Calendar caltmpDt = Calendar.getInstance();
							caltmpDt.setTime(tmpDate);
							int dayTmpDt = caltmpDt.get(Calendar.DATE);
							tmpVal= tmpVal.divide (new BigDecimal(dayTmpDt),2, ROUND_DECIMAL_DEFAULT);
							yearSum = yearSum.add(tmpVal);
							tmpVal = JClaretyConstants.ZERO_BIGDECIMAL;
							/***********************************************************************************
							If the Cost Calculation Date Day <> Last day of the month and YearCount = 1
							 ***********************************************************************************/
						}else if(termDateDay != termDateLastDay && yearCount == JClaretyConstants.ONE_INTEGER &&
								row != JClaretyConstants.ZERO_INTEGER){
							dayDiff = termDateDay - dayStrDt;
							dayDiff = dayDiff + 1;
							tmpVal = beBeneEstmtWage.wag_amt;
							tmpVal = tmpVal.multiply(new BigDecimal(dayDiff));
							tmpDate = becmnvaldn.getLastDayOfMnth (mnthStrDt ,yrStrDt);
							if(errorsPresent()){
								return null;
							}
							Calendar caltmpDt = Calendar.getInstance();
							caltmpDt.setTime(tmpDate);
							int dayTmpDt = caltmpDt.get(Calendar.DATE);
							tmpVal = tmpVal.divide (new BigDecimal(dayTmpDt), 2,ROUND_DECIMAL_DEFAULT);
							yearSum = yearSum.add(tmpVal);
							tmpVal = JClaretyConstants.ZERO_BIGDECIMAL;
							/****************************************************************************************
							Increase the YearCount, MonthCount by 1 and calculate YearSum and AvgCompAmt
							 ****************************************************************************************/	
						}else{
							yearSum = yearSum.add(beBeneEstmtWage.wag_amt);
						} 
					} 
					if(termDateDay != termDateLastDay && yearCount == 13){
						tmpBeAnlSal = new BEAnlSal();
						if(mnth < 10){
							tdTempDateAsTxt = "0";
							tdTempDateAsTxt = tdTempDateAsTxt.concat(String.valueOf(mnth));
						}else{
							tdTempDateAsTxt = String.valueOf(mnth);
						} 
						tdTempDateAsTxt=tdTempDateAsTxt.concat("/");
						if((termDateDay + 1) < 10){
							tdTempDateAsTxt = tdTempDateAsTxt.concat("0");
							tdTempDateAsTxt = tdTempDateAsTxt.concat(termDateDay+1+"");
						}else{
							tdTempDateAsTxt = tdTempDateAsTxt.concat(termDateDay+1+"");
						} 
						tdTempDateAsTxt=tdTempDateAsTxt.concat("/");
						tdTempDateAsTxt=tdTempDateAsTxt.concat(String.valueOf(yrStrDt));
						tmpBeAnlSal.strt_dt =  getValidateHelper().getDateFormatter().stringToDateMMDDYYYY(tdTempDateAsTxt);
						tmpBeAnlSal.end_dt = endDt;
						tmpBeAnlSal.anl_sal_amt = yearSum;
						paoBEAnlSal.add(tmpBeAnlSal);
						endDt = DateUtility.subtract(tmpBeAnlSal.strt_dt, oneDay);
						yearCount = 0;
						yearSum = JClaretyConstants.ZERO_BIGDECIMAL;
						row = row - 1;
					}else if(termDateDay == termDateLastDay && yearCount == 12){
						/***
						If the yearcount is 12, prepare a be_anl_sal record and append it to the array of be_anl_sal
						 ***/
						caltrmDt = Calendar.getInstance();
						caltrmDt.setTime(pCostCalcDt);
						Calendar calStrDtTemp = Calendar.getInstance();
						calStrDtTemp.setTime(beBeneEstmtWage.strt_dt);
						yrStrDt = calStrDt.get(Calendar.YEAR);
						mnth = caltrmDt.get(Calendar.MONTH)+1;
						if(mnth < JClaretyConstants.TEN_INTEGER){
							tdTempDateAsTxt = "0";
							tdTempDateAsTxt = tdTempDateAsTxt.concat(String.valueOf(mnth));
						}else{
							tdTempDateAsTxt= String.valueOf(mnth);
						}
						tdTempDateAsTxt = tdTempDateAsTxt.concat("/");
						//Check if it is a Leap year
						dayTrmDt =  caltrmDt.get(Calendar.DATE);
						if(dayTrmDt == JClaretyConstants.TWENTY_NINE_INTEGER && mnth==JClaretyConstants.TWO_INTEGER && 
								(yrStrDt%4) != JClaretyConstants.ZERO_INTEGER){
							tdTempDateAsTxt = tdTempDateAsTxt.concat(String.valueOf(JClaretyConstants.TWENTY_EIGHT_INTEGER));
						}else{
							tdTempDateAsTxt = tdTempDateAsTxt.concat(String.valueOf(dayTrmDt));
						}
						tdTempDateAsTxt = tdTempDateAsTxt.concat("/");
						tdTempDateAsTxt = tdTempDateAsTxt.concat(String.valueOf(yrStrDt));

						tmpBeAnlSal = new BEAnlSal() ;
						tmpBeAnlSal.strt_dt = getDateFormatter().stringToDateMMDDYYYY(tdTempDateAsTxt) ;

						//Add 1 day to the cost calculation date date.So that 03/31 becomes 04/01.
						tmpBeAnlSal.strt_dt = DateUtility.add(tmpBeAnlSal.strt_dt, oneDay);
						tmpBeAnlSal.end_dt = endDt;
						tmpBeAnlSal.anl_sal_amt = yearSum;
						paoBEAnlSal.add(tmpBeAnlSal);
						endDt = DateUtility.subtract(tmpBeAnlSal.strt_dt, oneDay); 
						yearCount = 0;
						yearSum = JClaretyConstants.ZERO_BIGDECIMAL;
					} 
				}  
			} 
		}
		BVFabmGBEPrepareAnlSalNCalaAvgComp bvFabmGBEPrepareAnlSalNCalaAvgComp = new BVFabmGBEPrepareAnlSalNCalaAvgComp();
		//1A@NE.02 - comment added
		// If the member didn't get the 12 months salary then paoBEAnlSal would be contain zero records.
		bvFabmGBEPrepareAnlSalNCalaAvgComp.paoBEAnlSal = paoBEAnlSal;
		bvFabmGBEPrepareAnlSalNCalaAvgComp.pBlobErr = pBlobErr;

		addTraceMessage("prepareAnlSalary", FABM_METHODEXIT);
		takeBenchMark("prepareAnlSalary", FABM_METHODEXIT);
		return bvFabmGBEPrepareAnlSalNCalaAvgComp;		
	}
	/**
	 
	 * Change History
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On           Description
	 * ====   ==============    =========   ==========  ==================================
	 * NE.02					Lashmi      10/12/2008   Initiated Coding
	 * P.01		PIR6134			vgp			05/13/2015	Re-wrote to use NPRIS LOB DB instead of  
	 * 													work flow system for peformance
	 *************************************************************************************/
	
	public boolean checkForRAP(BEMbrAcct pBEMbrAcct)
			throws ClaretyException {
	
		addTraceMessage("checkForRAP", FABM_METHODENTRY);
		startBenchMark("checkForRAP", FABM_METHODENTRY);
		Assert.notNull("BEMbrAcct", pBEMbrAcct);
		//P.01 Rewrite to remove WorkFlow system dependence for performance
		List<BEPrcs> aoBVWrkList = null;
		BEPrcs bePrcs = null;
		List<String> rapPrcsCds = Arrays.asList("RPRR", "RPDR");
		
		for (String rapPrcsCd : rapPrcsCds) {
			aoBVWrkList = dbPrcs.readByPrcsCdAcctId(rapPrcsCd, pBEMbrAcct.acct_id);
			if (errorsPresent()) {
				return false;
			}

			if (aoBVWrkList != null && aoBVWrkList.size() > 0) {
				for (int i = 0; i < aoBVWrkList.size(); i++) {
					bePrcs = aoBVWrkList.get(i);
					if (bePrcs.work_prcs_stat_cd.trim().equalsIgnoreCase(
							JClaretyConstants.WORK_PRCS_STAT_CD_COMPLETED)
							|| bePrcs.work_prcs_stat_cd
									.trim()
									.equalsIgnoreCase(
											JClaretyConstants.WORK_PRCS_STAT_CD_CANCELLED)) {
						continue;
					} else {
						return true;
					}
				}
			}
		}
		//P.01 - END
//        List aoBVWrkList  = this.getAllInitiatedPrcs();
//        BVWorkList bvwrklist = null;
//		
//		if(errorsPresent()) {
//			return false;
//		} 
//		if (aoBVWrkList != null && aoBVWrkList.size() > 0)
//		{
//			BEPrcs bePrcs = null;
//			BEWorkStatRef beWorkStatRef = null;
//			DBWorkStatRef dbWorkStatRef = (DBWorkStatRef)getDBInstance(DBWorkStatRef.class);
//			for (int i = 0; i < aoBVWrkList.size(); i++)
//			{
//				bvwrklist = (BVWorkList)aoBVWrkList.get(i);
//				beWorkStatRef = null;
//				if(bvwrklist != null)
//				{
//
//					bePrcs = dbPrcs.readByPrcsId(bvwrklist.prcs_id);
//					if (errorsPresent()){ 
//			   		return false;
//			   		}
//			   		if(bePrcs != null)
//			   		{
//			   		 if(bePrcs.acct_id == pBEMbrAcct.acct_id)
//			   		 {
//			   			beWorkStatRef = dbWorkStatRef.readByWorkStatCd(bePrcs.work_prcs_stat_cd);
//			   			if(errorsPresent())
//			   			{
//			   				return false;
//			   			}
//			   			if(beWorkStatRef != null && beWorkStatRef.work_stat_cd != null)
//			   			{
//			   				if( beWorkStatRef.work_stat_cd.trim().equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_COMPLETED)
//			   				  ||beWorkStatRef.work_stat_cd.trim().equalsIgnoreCase(JClaretyConstants.WORK_PRCS_STAT_CD_CANCELLED))
//			   				{
//			   					continue;
//			   				}
//			   				else
//			   				{
//			   					return true;
//			   				}
//			   			}
//			   		 }
//			   		}
//					
//				}//if bvwrklist != null
//				
//				
//			}// end of for loop for aoBVWrkList
//			return false;
//			
//		}//end of aoBVWrkList != null
	
		addTraceMessage("checkForRAP", FABM_METHODEXIT);
		takeBenchMark("checkForRAP", FABM_METHODEXIT);
		return false;
	}
	
	/*********************************************************
	 * Method Name : createRcvblsForEmpr()
	 *
	 * Input Parameters :
	 *		P_BEPln				:	BE_Pln
	 *		P_BVAgrmt			:	BV_Agrmt
	 *		P_EffDt				:	DateTimeData
	 *		P_PstngDt			:	DateTimeData
	 *
	 * Output Parameters :
	 *	BEAcctRcvbl
	 *
	 * Purpose :
	 *	create a AR for employer
	 * ===================================================================================
	 *        CCR#              Updated     Updated
	 * Ver    PIR#              By          On          Description
	 * ====   ==============    =========   ==========  ==================================
	 * P.01	  CCR 77795			gpannu	   10/26/2009	created helper method to create a AR for employer
	 * 													if agrmnt is prorated or completed and employer 
	 * 													contributions are still due.
	 * P.02	  CCR 77795			singhp		12/03/2009	Changed signature for updating paid pre tax and paid post tax amount.
	 * P.03	 CCR77795		    singhp		01/27/2010  to save tot_ipyr_amt and tot_mbr_int_pstd
	 * P.04   CCR77795			singhp		01/29/2010	Fix for removing the code added for P.03 to save into new table for CCR45
	 * P.05   NPRIS-5153		kgupta		04/16/2010	Modified code for the purpose of setting the busn_acct_id of be_fnc_item for 'EMAR' doc_typ_cd.
	 * P.06	NPRIS-5485			vgp			02/07/2012	Do not allow any tolerance for EPYR and IPYR portions of MKUP
	 *************************************************************************************/
	public BVAgrmtARLink createRcvblsForEmpr(BEPln pBEPln,BVAgrmt pBVAgrmt,Date pEffDt, Date pPstngDt,BEOrg pBeOrg) throws ClaretyException {

		startBenchMark("createRcvblsForEmpr", FABM_METHODENTRY);
		addTraceMessage("createRcvblsForEmpr",FABM_METHODENTRY);
		BEFncItem beFncItem = new BEFncItem();
		BEFncItem tmpBEFncItem;
		BVAgrmtARLink bvAgrmtARLink = new BVAgrmtARLink();//1A@P.02
		BEAcctRcvbl beacctrcvbl = new BEAcctRcvbl();
		BigDecimal rcvblAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		//BigDecimal itemAmount = JClaretyConstants.ZERO_BIGDECIMAL;	//2D@P.06
		//BigDecimal allocAmount = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal iPyrAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal ePyrAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoToFncItemsEmployer = new ArrayList();
		List aoToFncItemsInterest = new ArrayList();		
		BEFncDoc pFncDoc = new BEFncDoc();
		pFncDoc.fnc_doc_id = pBVAgrmt.agrmt.fnc_doc_id;
		List aoFncItem = dbfncitem.readAllByFncDoc(pFncDoc);//read all contributions from fnc item
		if(errorsPresent()){
			return null;
		}
		Date peffDt  = getTimeMgr().getCurrentDateOnly();//3A@P.03
		BigDecimal LINTAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		List aoBEIntItem = new ArrayList();
		
		for(int k =0; k<aoFncItem.size();k++){
			tmpBEFncItem = (BEFncItem)aoFncItem.get(k);
			//A@P.06 Start
			if (tmpBEFncItem != null
					&& !(tmpBEFncItem.item_typ_cd.trim()
							.equals(JClaretyConstants.FNC_ITEM_TYP_CD_MPYR))
					&& (tmpBEFncItem.item_amt
							.compareTo(tmpBEFncItem.to_alloc_amt) > JClaretyConstants.ZERO_INTEGER)) {
				if(tmpBEFncItem.item_typ_cd.trim().equals(JClaretyConstants.FNC_ITEM_TYP_CD_IPYR)){
					aoToFncItemsInterest.add(tmpBEFncItem);
					iPyrAmt = tmpBEFncItem.item_amt.subtract(tmpBEFncItem.to_alloc_amt);
				} else {
					aoToFncItemsEmployer.add(tmpBEFncItem);
					ePyrAmt = tmpBEFncItem.item_amt.subtract(tmpBEFncItem.to_alloc_amt);
				}
			}
			//A@P.06 End
			//D@P.06 Start
//			if(tmpBEFncItem!=null && !(tmpBEFncItem.item_typ_cd.trim().equals(JClaretyConstants.FNC_ITEM_TYP_CD_MPYR))){//add up only employer and int contributions
//				allocAmount = allocAmount.add(tmpBEFncItem.to_alloc_amt);
//				itemAmount = itemAmount.add(tmpBEFncItem.item_amt);
//				if(tmpBEFncItem.item_typ_cd.trim().equals(JClaretyConstants.FNC_ITEM_TYP_CD_IPYR)){
//					aoToFncItemsInterest.add(tmpBEFncItem);
//					if (tmpBEFncItem.item_amt.compareTo(tmpBEFncItem.to_alloc_amt) > JClaretyConstants.ZERO_INTEGER) {
//						iPyrAmt = tmpBEFncItem.item_amt;
//					}
//				}else{
//					aoToFncItemsEmployer.add(tmpBEFncItem);
//					if (tmpBEFncItem.item_amt.compareTo(tmpBEFncItem.to_alloc_amt) > JClaretyConstants.ZERO_INTEGER) {
//						ePyrAmt = tmpBEFncItem.item_amt;
//					}
//				}
//			}
			//D@P.06 End
		}
		if(errorsPresent()){
			return null;
		}
		
						
		//rcvblAmt = itemAmount.subtract(allocAmount);	//1D@P.06
		rcvblAmt = iPyrAmt.add(ePyrAmt);	//1A@P.06
		//if the rcvbl amount is greater than zero than only create a AR for Employer
		if(rcvblAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL)>JClaretyConstants.ZERO_INTEGER){
			BEDocStatHist bedocstathist = new BEDocStatHist();
			BVRetSys bvretsys;
			
			bedocstathist.doc_stat_cd = JClaretyConstants.DOC_STAT_CD_E;
			bedocstathist.doc_stat_dt = pEffDt;
			//Build Acct Rcvbl
			beacctrcvbl.doc_dt = pEffDt;
			beacctrcvbl.doc_fsc_yr = dbfscyr.calculateFiscalYearNr(pBEPln, pEffDt);
			if (errorsPresent()) {
				return null;
			}
			beacctrcvbl.doc_typ_cd = "EMAR";
			if (errorsPresent()) {
				return null;
			}
			// 4A@P.05
			BEEmpr beEmpr = dbempr.readByOrgId(pBeOrg.org_id);
			if(errorsPresent()){
				return null;
			}
			// create new receivable
			// 2M@P.05 - passed the object of beEmpr and null for beOrg.
			beacctrcvbl = fabmwageandcntrb.saveAcctRcvbl(beacctrcvbl, pBEPln, beEmpr,	bedocstathist, null, rcvblAmt,
					pPstngDt, pEffDt, null,true, beacctrcvbl.doc_typ_cd);
					
			if (errorsPresent()) {
				return null;
			}// End of receivable creation
			//save the ebtry in link table
			//12A@P.03//12D@P.04
			/*aoBEIntItem = dbintitem.readAllIntItemByEffDt(pBVAgrmt.mbr_acct, pEffDt, 0);
			if(errorsPresent()){
				return null;
			}
			if(aoBEIntItem != null){
				for(int i=0;i<aoBEIntItem.size();i++){
					BEIntItem beIntItem = (BEIntItem)aoBEIntItem.get(i);
					if(beIntItem.item_typ_cd.trim().equalsIgnoreCase(JClaretyConstants.ACCT_TRANS_TYP_CD_LINT)){
						LINTAmt = beIntItem.item_amt;
						break;
					}
				}
			}*/
			
			
				bvAgrmtARLink.agrmt_id = pBVAgrmt.agrmt.agrmt_id;
				bvAgrmtARLink.beAcctRcvbl.acct_rcvbl_id = beacctrcvbl.acct_rcvbl_id;
				bvAgrmtARLink.rcvbl_amt = rcvblAmt;
				bvAgrmtARLink.tot_ipyr_amt = JClaretyConstants.ZERO_BIGDECIMAL;//2A@P.03
				bvAgrmtARLink.tot_mbr_int_pstd = JClaretyConstants.ZERO_BIGDECIMAL;
				dbagrmt.saveAgrmtARLink(bvAgrmtARLink);
				if (errorsPresent()) {
					return null;
				}
			
			
			
			//update the IPYR and EPYR alloc amount with item amount as AR for employer is already created
			BEFncDoc beFncDoc=new BEFncDoc();
			beFncDoc.fnc_doc_id=beacctrcvbl.fnc_doc_id;
			List frombeFncItem = dbfncitem.readAllByFncDoc(beFncDoc);
			/*BEBusnAcct pBusnAcct = new BEBusnAcct();
			pBusnAcct.busn_acct_id = pBVAgrmt.mbr_acct.busn_acct_id;
			for(int k =0; k<aoFncItem.size();k++){
				tmpBEFncItem = (BEFncItem)aoFncItem.get(k);
				if(tmpBEFncItem!=null && !(tmpBEFncItem.item_typ_cd.trim().equals(JClaretyConstants.FNC_ITEM_TYP_CD_MPYR))){//save only employer and int contributions
				tmpBEFncItem.item_amt = tmpBEFncItem.to_alloc_amt;
				dbfncitem.save(tmpBEFncItem, pFncDoc, pBusnAcct);
				}
			}*/
			Date tempDt= getTimeMgr().getCurrentDateTime();
			
			//entry to update the EPYR in fnc item
			BVFABMGLTransallocateItems bvfabmTransallocateItems 
			= fabmgltrans.allocateItems(frombeFncItem, aoToFncItemsEmployer,
					null, pEffDt,
					tempDt);
			if(errorsPresent()){
				return null;
			}
			frombeFncItem=	bvfabmTransallocateItems.aoFromFncItems;
			
			//entry to update the IPYR in fnc item
			bvfabmTransallocateItems 
			= fabmgltrans.allocateItems(frombeFncItem, aoToFncItemsInterest,
					null, pEffDt,
					tempDt);
			if(errorsPresent()){
				return null;
			}
		}
			
		addTraceMessage("createRcvblsForEmpr", FABM_METHODEXIT);
		takeBenchMark("createRcvblsForEmpr", FABM_METHODEXIT);
		return bvAgrmtARLink;//1M@P.02
	}
	
	/* Change History
	 * ===================================================================================
	 *		CCR#            Updated     Updated
	 * Ver	PIR#            By          On           Description
	 * ====	==============	=========   ==========  ==================================
	 * P.01	NPRIS-5247		vgp			07/16/2010  Initial Coding
	 * P.02	NPRIS-5488		vgp			02/07/2012	GL changes for excess OSC interest  
	 *************************************************************************************/
	
	public void processEMARAdj(BEFncDoc pBEFncDoc, BigDecimal pChngIpyrAmt, int pFscYrNr, 
			boolean pIncIn, Date pPayPeriodDate, BEOrg pRetSysOrg, BEPln bePln, BEEmpr beEmpr)
			throws ClaretyException {
	
		addTraceMessage("processEMARAdj", FABM_METHODENTRY);
		startBenchMark("processEMARAdj", FABM_METHODENTRY);
		DBFncItem dbFncItem = (DBFncItem)getDBInstance(DBFncItem.class);
		DBFncAlloc dbFncAlloc = (DBFncAlloc) getDBInstance(DBFncAlloc.class);
		FABMWageAndCntrb fabmWageAndCntrb = (FABMWageAndCntrb)getFABMInstance(FABMWageAndCntrb.class);
		FABMGLTrans fabmGLTrans = (FABMGLTrans)getFABMInstance(FABMGLTrans.class);
		DBAcctRcvbl dbAcctRcvbl =(DBAcctRcvbl)getDBInstance(DBAcctRcvbl.class);
		DBAgrmt dbAgrmt =(DBAgrmt)getDBInstance(DBAgrmt.class);
		DBMbrAcct dbMbrAcct =(DBMbrAcct)getDBInstance(DBMbrAcct.class);
		DBMbr dbMbr =(DBMbr)getDBInstance(DBMbr.class);
				
		List aoBEFncAlloc = null; 
		BEAcctRcvbl beAcctRcvbl = null;
		BEAgrmtMbrIntPstdLink beAgrmtMbrIntPstdLink = null;
		BigDecimal initIpyrRcvblAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		//5D@P.02
//		BigDecimal currIpyrRcvblAmt = JClaretyConstants.ZERO_BIGDECIMAL;
//		BigDecimal prevIpyrRcvblAmt = JClaretyConstants.ZERO_BIGDECIMAL;
//		BigDecimal eMARIpyrEmprAmt = JClaretyConstants.ZERO_BIGDECIMAL;
//		BigDecimal totPaidIpyrAmt = JClaretyConstants.ZERO_BIGDECIMAL;
//		BigDecimal postedToMemberIpyrAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal addtnlGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		BigDecimal overPaidGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
		
		if (pBEFncDoc == null || pChngIpyrAmt == null || pChngIpyrAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) <= JClaretyConstants.ZERO_INTEGER) {	//1M@P.02
			return;
		}
		
		List aoBEFncItems = dbFncItem.readAllByFncDoc(pBEFncDoc);
		if(errorsPresent()){
			return;
		}
		
		for(int i = 0; i < aoBEFncItems.size(); i++){
			BEFncItem beFncItem = (BEFncItem)aoBEFncItems.get(i);
			aoBEFncAlloc = dbFncAlloc.readByFromItem(beFncItem);
			if(errorsPresent()){
				return;
			}
			if(aoBEFncAlloc != null && aoBEFncAlloc.size() > 0){
				for(int j = 0; j < aoBEFncAlloc.size(); j++){
					BEFncAlloc beFncAlloc = (BEFncAlloc)aoBEFncAlloc.get(j);
					if(beFncAlloc.alloc_typ_cd.trim().equals(JClaretyConstants.ALLOC_TYP_CD_ARMI)){
						initIpyrRcvblAmt = initIpyrRcvblAmt.add(beFncAlloc.alloc_amt);
					}
				}
			}
		}
		// if the initial AR amount allocated to ipyr is zero then no additional GL needed
		if (initIpyrRcvblAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL)== JClaretyConstants.ZERO_INTEGER) {
			return;
		}
		
		//15D@P.02
//		prevIpyrRcvblAmt = dbAcctRcvbl.sumEMARIpyrAdjTotalByFncDoc(pBEFncDoc);
//		if(errorsPresent()|| prevIpyrRcvblAmt == null) {
//			return;
//		}
//		
//		//EMAR interest portion paid by employer so far
//		eMARIpyrEmprAmt = dbAcctRcvbl.sumEMARIpyrPaidByEmpr(pBEFncDoc);
//		if(errorsPresent()) {
//			return;
//		}
//		
//		if (pIncIn) {
//			currIpyrRcvblAmt = prevIpyrRcvblAmt.add(pChngIpyrAmt);
//		} else {
//			currIpyrRcvblAmt = prevIpyrRcvblAmt.subtract(pChngIpyrAmt);
//		}
		
		beAcctRcvbl = dbAcctRcvbl.readAcctRcvblByFncDoc(pBEFncDoc);
		if(errorsPresent()|| beAcctRcvbl == null) {
			return;
		}
		
		beAgrmtMbrIntPstdLink = dbAgrmt.readMbrIntPstdLinkbyAcctRcvbl(beAcctRcvbl);
		if(errorsPresent()|| beAgrmtMbrIntPstdLink == null || beAgrmtMbrIntPstdLink.tot_ipyr_amt == null || beAgrmtMbrIntPstdLink.tot_mbr_int_pstd == null) {
			return;
		}
		//13D@P.02
//		//Total ipyr paid by employer
//		if (beAgrmtMbrIntPstdLink.tot_ipyr_amt.compareTo(initIpyrRcvblAmt) > JClaretyConstants.ZERO_INTEGER) {
//			totPaidIpyrAmt = beAgrmtMbrIntPstdLink.tot_ipyr_amt.add(eMARIpyrEmprAmt).subtract(initIpyrRcvblAmt);
//		} else {
//			totPaidIpyrAmt = eMARIpyrEmprAmt;
//		}
		
//		//total ipyr posted to member's account
//		if (beAgrmtMbrIntPstdLink.tot_ipyr_amt.compareTo(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd) < JClaretyConstants.ZERO_INTEGER) {
//			postedToMemberIpyrAmt = beAgrmtMbrIntPstdLink.tot_ipyr_amt;
//		} else {
//			postedToMemberIpyrAmt = beAgrmtMbrIntPstdLink.tot_mbr_int_pstd;
//		}
		//A@P.02 Start
		if (pIncIn) {
			//if paid ipyr is > psoted amt then all the increased amount should be moved from 2151 to 9332 
			if (beAgrmtMbrIntPstdLink.tot_ipyr_amt.compareTo(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd) >= JClaretyConstants.ZERO_INTEGER) {
				addtnlGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				overPaidGLAmt = pChngIpyrAmt;
			} else if (beAgrmtMbrIntPstdLink.tot_ipyr_amt.add(pChngIpyrAmt).compareTo(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd) <= JClaretyConstants.ZERO_INTEGER) {
				addtnlGLAmt = pChngIpyrAmt;
				overPaidGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
			} else {
				addtnlGLAmt = beAgrmtMbrIntPstdLink.tot_mbr_int_pstd.subtract(beAgrmtMbrIntPstdLink.tot_ipyr_amt);
				overPaidGLAmt = pChngIpyrAmt.subtract(addtnlGLAmt);
			}
		} else {
			if (pChngIpyrAmt.compareTo(beAgrmtMbrIntPstdLink.tot_ipyr_amt) > JClaretyConstants.ZERO_INTEGER )	{
				pChngIpyrAmt = beAgrmtMbrIntPstdLink.tot_ipyr_amt;
			}
			if (beAgrmtMbrIntPstdLink.tot_ipyr_amt.compareTo(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd) <= JClaretyConstants.ZERO_INTEGER) {
				addtnlGLAmt = pChngIpyrAmt;
				overPaidGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
			} else if ((beAgrmtMbrIntPstdLink.tot_ipyr_amt.subtract(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd)).compareTo(pChngIpyrAmt) >= JClaretyConstants.ZERO_INTEGER) {
				addtnlGLAmt = JClaretyConstants.ZERO_BIGDECIMAL;
				overPaidGLAmt = pChngIpyrAmt;
			} else {
				overPaidGLAmt = beAgrmtMbrIntPstdLink.tot_ipyr_amt.subtract(beAgrmtMbrIntPstdLink.tot_mbr_int_pstd);
				addtnlGLAmt = pChngIpyrAmt.subtract(overPaidGLAmt);
			}
		}
		//A@P.02 End
		//D@P.02 Start
//		if (pIncIn && totPaidIpyrAmt.add(prevIpyrRcvblAmt).compareTo(postedToMemberIpyrAmt) < JClaretyConstants.ZERO_INTEGER) {
//			addtnlGLAmt = postedToMemberIpyrAmt.subtract(totPaidIpyrAmt.add(prevIpyrRcvblAmt));	
//		} else if ((!pIncIn) && totPaidIpyrAmt.add(currIpyrRcvblAmt).compareTo(postedToMemberIpyrAmt) < JClaretyConstants.ZERO_INTEGER) {
//			addtnlGLAmt = postedToMemberIpyrAmt.subtract(totPaidIpyrAmt.add(currIpyrRcvblAmt));
//		}
//		
//		if (addtnlGLAmt.compareTo(pChngIpyrAmt) > JClaretyConstants.ZERO_INTEGER) {
//			addtnlGLAmt = pChngIpyrAmt;
//		}
		//D@P.02 End			
		//Now has determined that there is need for additional GL entries
		if (addtnlGLAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER ||	//1M@P.02
				overPaidGLAmt.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) > JClaretyConstants.ZERO_INTEGER) {	//1A@P.02
			Date currentDate = getTimeMgr().getCurrentDateTime();
			BEMbrAcct beMbrAcct = dbMbrAcct.readByAcctId(beAgrmtMbrIntPstdLink.acct_id);
			if(errorsPresent() || beMbrAcct == null){
				return;
			}
			BEMbr beMbr = dbMbr.readByMbrAcct(beMbrAcct);
			if(errorsPresent()|| beMbr == null){
				return;
			}
			fabmGLTrans.postGLForEMARAdj(bePln, currentDate,addtnlGLAmt, beMbr, currentDate, pIncIn, overPaidGLAmt);	//1M@P.02
		}
		//5A@P.02
		//Update beAgrmtMbrIntPstdLink
		beAgrmtMbrIntPstdLink.tot_ipyr_amt =  ((pIncIn)?beAgrmtMbrIntPstdLink.tot_ipyr_amt.add(pChngIpyrAmt): beAgrmtMbrIntPstdLink.tot_ipyr_amt.subtract(pChngIpyrAmt));
		dbAgrmt.saveAgrmtMbrIntPstdLink(beAgrmtMbrIntPstdLink);
		if(errorsPresent()) {
			return;
		}
		
		addTraceMessage("processEMARAdj", FABM_METHODEXIT);
		takeBenchMark("processEMARAdj", FABM_METHODEXIT);
		return;
	}
	
	/*
	* Description : Check if any previous refund exists for a member
	* Change History
	* ===================================================================================
	*        CCR#            Updated    	  Updated
	* Ver    PIR#              By         		On         Description
	* ====   ===========    ===========  	==========  =============================
	* P.01	 NPRIS-6481		 VenkataM		10/12/2017	  Initial Coding.	
	**************************************************************************************/
	public boolean checkRfndExistsForMbrAcct(BEMbrAcct beMbrAcct) throws ClaretyException {
		boolean rfndExists = false;
		List<BEAcctTrans> aoBEAcctTrans = dbAcctTrans.readRefundNBuybackTransForMbrAcct(beMbrAcct);
		if (aoBEAcctTrans != null && aoBEAcctTrans.size() > JClaretyConstants.ZERO_INTEGER) {
			BigDecimal sumRefundNBuybackSC = JClaretyConstants.ZERO_BIGDECIMAL;
			for (BEAcctTrans lclBEAcctTrans : aoBEAcctTrans) {
				sumRefundNBuybackSC = sumRefundNBuybackSC.add(lclBEAcctTrans.srv_crdt_net_amt);
			}
			// If sumRefundNBuybackSC is -ve value means that there is still
			// refund service that has not been purchased.
			if (sumRefundNBuybackSC.compareTo(JClaretyConstants.ZERO_BIGDECIMAL) < JClaretyConstants.ZERO_INTEGER) {
				rfndExists = true;
			}
		}
		return rfndExists;
	}
}
