/*
 *  tibs_bill/local_tj/tj_bss_funds.cpp
 *	CRM预存接口(支持批量预存[支持返还]);
 *
 -----------------------------------------------------------------------
 *  ver 1.0.0 @2009.02.26;	Created
 -----------------------------------------------------------------------
 *  ver 1.1.0 @2009.02.28;  增加`insertBssFundsLog':记录交易日志(含失败的)A_Bss_Funds_log ;
 ---------------------------------------------------------------------------
 *  ver 1.1.1 @2009.02.28;  增加信号处理`sigset(SIGTERM, SigProcess_Bss)';
 ----------------------------------------------------------------------------
 *  ver 1.2.0 @2009.03.01;  利用交易日志A_Bss_Funds_log控制 重复交易,活动互斥等;
 ----------------------------------------------------------------------------
 *  ver 1.5.0 @2009.03.02;  增加`checkVirmProcessID' 避免起多个重复进程;
 ----------------------------------------------------------------------------
 *  ver 1.5.1 @2009.03.03;  增加`gethostname,getcwd' 避免起多个重复进程;
 ----------------------------------------------------------------------------
 *  ver 1.5.2 @2009.03.04;  增加`C_bss_rule_map_prs::IsServIDExist'
						 	#Serv_ID对应的资料进计费系统有延迟的 先跳过;
 ----------------------------------------------------------------------------
 *  ver 1.5.3 @2009.03.05;  去掉"From Rent where START_DATE <= sysdate "限制;
 ----------------------------------------------------------------------------
 *  ver 1.5.5 @2009.03.06;  判断是否要在凌晨重新连接数据库;读取sessionInfo;
 							lCtrlFlag:(0X01+0X02 + 0X10)测试重连DB取SessionInfo;
 							lCtrlFlag:(0X01+0X02)完美功能;
 ----------------------------------------------------------------------------
 
 和BSS(CRM)的Rent.Pay_Plan_ID匹配原则(映射关系)需要确认; 
 ----------------------------------------------------------------------------
 Compile :/acct/tibs_bill/local_tj> 
 ---------------------------------------------------------------------------
 */

////#define  DEBUG

//#include "stdlib.h" 
#include "BILL_INCLUDE.h"
#include "tblflds.h"
#include "appflds.h"
#include "C_AGENT_BILL_PROCESS_JS.h"
#include "C_SEARCH_ENGINE_PROCESS.h"	 
#include "C_INTER_COMMON_JS.h"
#include  "CashBill_BO.h"
///#include  "c_query_log_js.h"


////----- sleep(unsigned int iSeconds )  ---
#include <unistd.h>
#include <signal.h>

#include "C_SYSTEM_SWITCH.h"
#include "abmCommon.h"
using namespace std;

///////引用外部变量;
//
extern Connection *gpDBLink;
extern char* Number2cSTR(const  oracle::occi::Number   &Value);

static C_OCCI_DATABASE  o_BSS_db;  ///--BSS：取支付计划数据
static C_OCCI_DATABASE  o_ACCT_db; ///--计费：充值
Connection *gpBSS_DBLink = NULL ;
Connection *gpACCT_DBLink = NULL ;

///////////////////////////////////////////////////////////
#define BSS_TJ_PAY_PLAN_NOTFOUND  "961021"  /*找不到匹配的支付计划PAY_PLAN*/
#define BSS_TJ_SERV_ID_NOT_MATCH  "961022"  /*Serv_ID不匹配*/
#define BSS_FUNDS_LOG_SUCC_EXIST  "961023"  /*A_BSS_FUNDS_LOG存在成功记录*/

#define INSERT_ASCAN_PAYMENT_FAIL	"5128"			/* 插入ASCAN_PAYMENT表出错 */
#define INSERT_APAYMENT_INFO_EXT_FAIL	"5129"			/* 插入APAYMENT__INFO_EXT表出错 */

#define JSLIB_DATE_TIME_FORMAT	"yyyymmddhhmiss"
#define TJ_BSS_RENT_STATE_NORMAL	"N"
#define TJ_BSS_RENT_STATE_DEALED	"D"
#define TJ_BSS_RENT_STATE_Failed	"E"

//////////
class TJ_BssPay_Info_t;
class  RuleMap_Info_t;
class  C_bss_rule_map_prs;
class  C_TJ_BSS_Funds_mgr;
string   bss_funds_inter_One( TJ_BssPay_Info_t& tTBssPayInfo ,string  &rsContext );
string   BSS_FUNDS( const long& rlRecLimit ,long& rlBssPayCnt ,long& rlDynamic);

///////局部变量;
static	long	glMOBILE_PROXY_STAFF_ID = 3 ;
static  long 	glProcessID = 0;
static  bool 	gbNormalRunFlag = false ;

////@2009.03.03;  
static	long	glServID = -1;
static	long	glPROD_2_PP_ID = -1;
string  gstrHostName="-1" ;

static vector<RuleMap_Info_t*> gvRuleMapInfo_vt;


class TJ_BssPay_Info_t
{
  public:
  	long m_lPROD_2_PP_ID; //unique index IDX_RENT_PP_ID on RENT (PROD_2_PP_ID)
  	long m_lADDUP_SUMLIMIT;	//合约款
	
  	long lPayPlanID;
	long lServID;
	long lAmount;

	///--bss_funds_inter_One()函数回吐的字段--;
	long lPaymentID;
	long lRTP_NBR;
	long lAcctID ;
	char sAccNbr[32];
	
	//add by pengqiang5 for TJ 快补
	long m_lTerminalAmount;		//终端款
	long m_lDiscRate;			//终端款所占合约比率
	long m_lKBFlag;				//快补标志位
	
	//add to info_ext table
	char PayacctName[80];
	char CheckNum[80];
	char Pay_date[19];
	char Bankacct_type[500];
	char Bank_type[500];
	char BankacctNum[500];

	//add to ascan table
	char ScanpaymentID[18];
};


class  RuleMap_Info_t
{
  public:
	long  lRTP_NBR		;
	long  lBSS_PAY_PLAN_ID ;
	long  lCHARGE          ;
	long  lBALANCE_TYPE_ID ;
	long  lRETURN_RULE_ID  ;
	long  lCONFER_FLAG     ;
	long  lConferBalanceTypeID;
	char  sDEPOSIT_TYPE[8]    ;
	long  lCYCLE_UPPER     ;
	long  lCYCLE_LOWER     ;
	char  sEFF_DATE[19]        ;
	char  sEXP_DATE[19]        ;
};

//添加另两个要插入表格的信息类
class AScanPay_Info_t
{
  public:
	char ScanpayID[30];
	long StaffID;
	char Areacode[10];
	long AcctID;
	long Amount;
	char Create_date[19];
	char Scantype;
	char Scanstate[3];
	char Scan_date[19];
	char Scanstate_date[19];
	char Scanchannel[10];
	char Scanerror[30];
	char Cashstate[3];
	char Cash_date[19];
	long CashpaymentID;
	int Rollflag;
	char Rollscan_ID[30];
};

class Apayment_Info_ext_t
{
	public:
	long PaymentID;
	long BalantypeID;
	char PayacctName[80];
	char CheckName[80];
	char Create_date[19];
	char Pay_date[19];
	char Bankacct_type[500];
	char Bank_type[500];
	char BankacctNum[500];
	char Crmpayserial[500];
	char Modify_date[19];
	int Source_type;
	char OlID[50];
	char Operation_type[3];
	int Defund_type;
	long OldpayID;
};


class C_bss_rule_map_prs : public C_OCCI_DBACCESS
{
  public:
	string	freshRuleMapInfo( vector<RuleMap_Info_t*>& rvBTL );
	string insertBssFundsLog(const long& lPROD_2_PP_ID
		,const long& lRTP_NBR ,const long& lPaymentID,const long& lServID 
		,const long& lAcctID  ,const char* sAccNbr ,const char* sState
		,const string& strContext);
	
	string insertAScanPayment(TJ_BssPay_Info_t* Bsspayinfo, const string& sScan_date, const string& sCash_date);
	string insertAPaymentExtent(const long& lPaymentID,const string& sCreate_date, TJ_BssPay_Info_t* Bsspayinfo);
	string getBssNbrSuccCnt(const long& lPROD_2_PP_ID ,long& rlFlag);
	string  checkVirmProcessID(const long& rlProcessID ,string& strExePathFile );
	string   updateVirmProcessByID(const long& rlProcessID);
	string IsServIDExist( const long& rlServID ,bool& rbExist );
	
	string getTablename_Date(string& strTableName);
	
	///string getDBSessionInfo(const char* sProgName ,const char*sHostName ,long& lSID ,string& sLogonTime  );
	string getDBSessionInfo(const char* sProgName ,string& sHostName 
							,long& lSID ,string& sLogonTime  );
  //add begin by xlfei on 20170420 for 增加未激活用户返赠款的处理
  string jugde_2hn_deposit_offer(const int& lOfferID, bool& bExist);
  //add end by xlfei on 20170420 for 增加未激活用户返赠款的处理
  protected:
	string	 qryRuleMap( );	
	string nextRuleMap( RuleMap_Info_t*& rpBRP ,bool& rbFlag);
  	string getRuleMapCnt( long& rlCnt );
	string	getRuleMapInfo( vector<RuleMap_Info_t*>& rvBTL );
	
public:
  C_bss_rule_map_prs(Connection *& db) : C_OCCI_DBACCESS(db)
  {
  }
  
  ~C_bss_rule_map_prs()
  {
  }

  private:
	char m_sDestTableName[81];
	
};

class C_TJ_BSS_Funds_mgr : public C_OCCI_DBACCESS
{
  public:
  	
	///template occi qry Muti Records; add@2008.12.18;
	string	getBssRentPayInfo( const long& rlRecLimit ,vector<TJ_BssPay_Info_t*>& rvBTL );
	string updateBssRentPay( const long& rlPROD_2_PP_ID ,const string& rstrState ); 
	string LockRentRow( const long& rlPROD_2_PP_ID  );
	
  protected:
	string	 queryBssRentPay( );	
	string NextBssRentPay(TJ_BssPay_Info_t*& rpBRP ,bool& rbFlag);

public:
  C_TJ_BSS_Funds_mgr(Connection *& db) : C_OCCI_DBACCESS(db)
  {
  }
  
  ~C_TJ_BSS_Funds_mgr()
  {
  }

  private:
	char m_sSrcTableName[81];
	
};

string C_TJ_BSS_Funds_mgr::queryBssRentPay()
{
	string sSQL;
	int iPosition = 1;
	
	try
	{
		sSQL.clear();
		sSQL  = "SELECT A.PROD_2_PP_ID, ";
		sSQL += "		A.PRICE_PLAN_CD, ";
		sSQL += "		A.PROD_ID, ";
		sSQL +=	"		NVL(A.ADDUP_SUMLIMIT,0), ";
		sSQL += "		NVL(A.TERMINAL_SPREAD,-1), ";
		sSQL += "		NVL(A.KB_FLAG,0), ";

		sSQL += "		A.PAY_ACCT_NAME, ";
		sSQL += "		A.CHECK_NUM, ";
		sSQL += "		TO_CHAR(A.PAY_DATE, 'YYYYMMDDHH24MISS'), ";
		sSQL += "		A.BANK_ACCT_TYPE, ";
		sSQL += "		A.BANK_TYPE, ";
		sSQL += "		A.BANK_ACCT_NUM, ";
		
		sSQL += "		A.SCAN_PAYMENT_ID ";
		sSQL += "  FROM RENT A  " ;
		//modify begin by xlfei on 20170330 for紧急需求,57925	酷视卡首月优惠（赠送10元话费）要求未激活也能返赠款
		sSQL += " WHERE (A.ADDUP_STATE = :BTL_State or (A.ADDUP_STATE = 'T' ";
		sSQL += "  and a.PRICE_PLAN_CD in (select offer_id from deposit_2hn_rent a where a.state = '10A')))" ;
   //modify end by xlfei on 20170330 for紧急需求,57925	酷视卡首月优惠（赠送10元话费）要求未激活也能返赠款
		///## ; add@2009.03.02;
		///## 此限制不需要; mdy@2009.03.03;
		///sSQL +="  AND  NVL(a.ADDUP_SUMLIMIT ,0) > 0 " ;
		if(-1 != glPROD_2_PP_ID ){
			sSQL += "   AND A.PROD_2_PP_ID = :PROD_2_PP_ID " ;
		}

		if(-1 != glServID){
			sSQL += "   AND A.PROD_ID = :PROD_ID " ;
		}
        
		///去掉"  AND  START_DATE <= sysdate "限制;
		///mdy@2009.03.05;
		/* 原因是 里面存在下面的记录没有处理导致 用户投诉 :-(
			select * from RENT where PRICE_PLAN_CD = 500000131 
	 		and  trunc(START_DATE) =to_date('20090401','yyyymmdd')
		*/
		sSQL += "   AND NVL(A.END_DATE ,sysdate+1) > sysdate " ;

		//##只处理12小时以内的; add@2009.03.04;
		///sSQL +=" AND CREATE_DATE < SYSDATE -12/24 "; 开始写错了:-)
		//add begin by xlfei on 20120327 for 计费不再处理预约生效的租机
		//sSQL +=" AND CREATE_DATE >= SYSDATE -12/24 ";
		sSQL += "   AND A.CREATE_DATE <= SYSDATE ";
        //add end by xlfei on 20120327 for 计费不再处理预约生效的租机
		
	
		sSQL += " ORDER BY A.PROD_2_PP_ID ASC ";

		newQuery();
		setSQL(sSQL);
#ifdef DEBUG
		C_Date oDate;
		fprintf(stderr,"File[%s]_Line[%d]_Time[%s]:SQL[%s]"
			,__FILE__  ,__LINE__ 
			,oDate.toString(JSLIB_DATE_TIME_FORMAT) ,STRING_2_CSTR(sSQL) );		
#endif

		setParameter( iPosition++ ,  TJ_BSS_RENT_STATE_NORMAL );
		if(-1 != glPROD_2_PP_ID ){
			 setParameter( iPosition++ ,   glPROD_2_PP_ID );
		}

		if(-1 != glServID){
			setParameter( iPosition++ ,   glServID);
		}
		
		executeQuery();

		return SUCCESS;
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),  ErrMsg[%s]" , __FILE__ , __LINE__ 
			,  oraex.getErrorCode(), oraex.getMessage().c_str() );
		 
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(exception &ex)
	{
		return  SELECT_BALANCE_TRANSFER_ERROR ;
	}
	catch(...)
	{
		return  SELECT_BALANCE_TRANSFER_ERROR;
	}
	
}

string C_TJ_BSS_Funds_mgr::NextBssRentPay(TJ_BssPay_Info_t*&rpBRP ,bool& rbFlag)
{
	int iCurPosition = 1;
	
	try
	{
		if (!m_rs->next())
		{
			closeResultSet();
			close();

			rbFlag =false;
			return SUCCESS;
		}
		
		rpBRP->m_lPROD_2_PP_ID = (long)m_rs->getNumber(iCurPosition++) ;
		rpBRP->lPayPlanID = (long)m_rs->getNumber(iCurPosition++) ;
		rpBRP->lServID = (long)m_rs->getNumber(iCurPosition++) ;
		rpBRP->lAmount = 0L; 
		rpBRP->m_lADDUP_SUMLIMIT = (long)m_rs->getNumber(iCurPosition++);
		rpBRP->m_lTerminalAmount = (long)m_rs->getNumber(iCurPosition++);
		rpBRP->m_lKBFlag = (long)m_rs->getNumber(iCurPosition++);
		if(rpBRP->m_lTerminalAmount > 0 && rpBRP->m_lKBFlag == 2L)
		{
			//快补返还流程
			if(rpBRP->m_lTerminalAmount >= rpBRP->m_lADDUP_SUMLIMIT)
			{
				//rpBRP->m_lADDUP_SUMLIMIT = rpBRP->m_lTerminalAmount;
				rpBRP->m_lTerminalAmount = rpBRP->m_lADDUP_SUMLIMIT;
			}
			
			rpBRP->m_lDiscRate = (long)ceil(((double)(rpBRP->m_lTerminalAmount)/(rpBRP->m_lADDUP_SUMLIMIT))*100000000);
		}
		else
		{
			//普通返还流程
			rpBRP->m_lDiscRate = -1L;
			//rpBRP->m_lKBFlag = 0L;
		}
		
		strcpy(rpBRP->PayacctName , m_rs->getString(iCurPosition++).c_str());
		strcpy(rpBRP->CheckNum , m_rs->getString(iCurPosition++).c_str());
		strcpy(rpBRP->Pay_date , m_rs->getString(iCurPosition++).c_str());
		strcpy(rpBRP->Bankacct_type , m_rs->getString(iCurPosition++).c_str());
		strcpy(rpBRP->Bank_type , m_rs->getString(iCurPosition++).c_str());
		strcpy(rpBRP->BankacctNum , m_rs->getString(iCurPosition++).c_str());
		
		strcpy(rpBRP->ScanpaymentID , m_rs->getString(iCurPosition++).c_str());

		///##
		rpBRP->lPaymentID = -1; 
		rpBRP->lRTP_NBR = -1; 
		rpBRP->lAcctID = -1; 
		strcpy( rpBRP->sAccNbr, "-1" ); 
		
		rbFlag =true;
		return SUCCESS;
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );			
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(...)
	{
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	
	return SUCCESS;
}

string	C_TJ_BSS_Funds_mgr::getBssRentPayInfo( const long& rlRecLimit 
			,vector<TJ_BssPay_Info_t*>& rvBTL )
{
	string sRet=SUCCESS;	
	TJ_BssPay_Info_t*	pBTL = NULL;
	
	try{
		DELETE_VECT_CLEAR(rvBTL);
		////打开游标;
		sRet= queryBssRentPay(  );
		if(sRet.compare(SUCCESS)){
			THROW_BILL(SYS_ERR_CODE, "", sRet);
		}

		do{
			bool bHaveDataFlag = false;
			pBTL = new TJ_BssPay_Info_t();
			if(!pBTL){
				THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR);
			}
			
			////取游标里面 1条记录;					
			sRet= NextBssRentPay( pBTL ,bHaveDataFlag);
			if(sRet.compare(SUCCESS)){
				THROW_BILL(SYS_ERR_CODE, "", sRet);
			}
			
			////游标对应的记录全部取结束;				
			if( !bHaveDataFlag){
				break;
			}
				
			rvBTL.push_back(pBTL);
			pBTL = NULL;

			if(rvBTL.size() >= rlRecLimit ){
				///已经取到足够的记录数;
				break;
			}
			
		} while(1);


	} 
	catch(BillException &bex)
	{
		sRet = bex.getContext();
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );			
		sRet = SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(...)
	{
		sRet =  SELECT_BALANCE_TRANSFER_ERROR;
	}

	RELEASE_CLASS_INSTANCE(pBTL);
	
	if(sRet.compare(SUCCESS)){
		DELETE_VECT_CLEAR(rvBTL);
	}

	return sRet;
}

string C_TJ_BSS_Funds_mgr::updateBssRentPay( const long& rlPROD_2_PP_ID 
	,const string& rstrState )
{
      string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;
		
        try
        {
			sSQL.clear();
			sSQL  = 
				"update Rent "
				" SET  ADDUP_STATE = :rstrState ,ADDUP_CHANGE_DATE =SYSDATE "
				" where PROD_2_PP_ID = :NBR " ;

			newQuery();
			setSQL(sSQL);
			
			setParameter(iCurPosition++ ,rstrState );	
			setParameter(iCurPosition++ ,rlPROD_2_PP_ID);					
			executeUpdate();
				
			close();				
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet = SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet = SELECT_BALANCE_TRANSFER_ERROR;
		}

        return sRet;

}

string C_TJ_BSS_Funds_mgr::LockRentRow( const long& rlPROD_2_PP_ID  )
{
       string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;
		
        try
        {
			sSQL.clear();
			sSQL  = "select 1 from Rent where PROD_2_PP_ID = :NBR "
				" FOR update nowait  " ;

			newQuery();
			setSQL(sSQL);
				
			setParameter(iCurPosition++ ,rlPROD_2_PP_ID);					
			executeQuery();
			m_rs->next() ;
				
			closeResultSet();
			close();				
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet = SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet = SELECT_BALANCE_TRANSFER_ERROR;
		}

        return sRet;

}

string	 C_bss_rule_map_prs::qryRuleMap( )
{
	string sSQL;
	int  iPosition = 1;
	
	try
	{
		sSQL.clear();
		sSQL =  " select  RTP_NBR	 " 
		 " ,BSS_PAY_PLAN_ID  " 
		 " ,CHARGE           " 
		 " ,BALANCE_TYPE_ID  " 
		 " ,RETURN_RULE_ID   " 
		 " ,CONFER_FLAG      " 
		 " ,CONFER_CHARGE    " 
		 " ,DEPOSIT_TYPE     " 
		 " ,NVL(CYCLE_UPPER ,-1)     " 
		 " ,NVL(CYCLE_LOWER ,-1)     " 
		 " , NVL( TO_CHAR(a.EFF_DATE, 'YYYYMMDDHH24MISS') ,' ') " 
		 " , NVL( TO_CHAR(a.EXP_DATE, 'YYYYMMDDHH24MISS') ,' ') " 
		 " FROM a_bss_funds_rule_map  a   " 
		 " order by  RTP_NBR ASC " 
		 ;

		newQuery();
		setSQL(sSQL);
#ifdef DEBUG
		C_Date oDate;
		fprintf(stderr,"File[%s]_Line[%d]_Time[%s]:SQL[%s]"
			,__FILE__  ,__LINE__ 
			,oDate.toString(JSLIB_DATE_TIME_FORMAT) ,STRING_2_CSTR(sSQL) );		
#endif

		///setParameter( iPosition++ ,  PAYMENT_CREATE_STATE);
		
		executeQuery();

		return SUCCESS;
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),  ErrMsg[%s]" , __FILE__ , __LINE__ 
			,  oraex.getErrorCode(), oraex.getMessage().c_str() );
		 
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(exception &ex)
	{
		return  SELECT_BALANCE_TRANSFER_ERROR ;
	}
	catch(...)
	{
		return  SELECT_BALANCE_TRANSFER_ERROR;
	}
}

string C_bss_rule_map_prs::nextRuleMap( RuleMap_Info_t*& rpBRP ,bool& rbFlag)
{
	int iCurPosition = 1;
	
	try
	{
		if (!m_rs->next())
		{
			closeResultSet();
			close();

			rbFlag =false;
			return SUCCESS;
		}
		
	rpBRP->lRTP_NBR		 = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lBSS_PAY_PLAN_ID         = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lCHARGE                  = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lBALANCE_TYPE_ID         = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lRETURN_RULE_ID          = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lCONFER_FLAG             = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lConferBalanceTypeID     = m_rs->getNumber(iCurPosition++) ;

	strcpy(rpBRP->sDEPOSIT_TYPE            , m_rs->getString(iCurPosition++).c_str() ) ;
	rpBRP->lCYCLE_UPPER             = m_rs->getNumber(iCurPosition++) ;
	rpBRP->lCYCLE_LOWER             = m_rs->getNumber(iCurPosition++) ;
	strcpy(rpBRP->sEFF_DATE                , m_rs->getString(iCurPosition++).c_str() ) ;
	strcpy(rpBRP->sEXP_DATE                , m_rs->getString(iCurPosition++).c_str() ) ;

		
		rbFlag =true;
		return SUCCESS;
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );			
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(...)
	{
		return SELECT_BALANCE_TRANSFER_ERROR;
	}
	
	return SUCCESS;

}

string C_bss_rule_map_prs::getRuleMapCnt( long& rlCnt )
{
        string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;

		rlCnt = 0;
		
        try
        {
			sSQL.clear();
			sSQL  = " select count(1) " 
                	" from  a_bss_funds_rule_map  " ;

				newQuery();
				setSQL(sSQL);
				
				///setParameter(iCurPosition++ ,rlServID);					
				executeQuery();
				if( m_rs->next() ){
					rlCnt = (long)m_rs->getNumber(1);
				}
				
				closeResultSet();
				close();				
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet = SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet = SELECT_BALANCE_TRANSFER_ERROR;
		}

        return sRet;
}


string	C_bss_rule_map_prs::getRuleMapInfo( vector<RuleMap_Info_t*>& rvBTL )
{
	string sRet=SUCCESS;	
	RuleMap_Info_t*	pBTL = NULL;
	
	try{
		DELETE_VECT_CLEAR(rvBTL);
		////打开游标;
		sRet= qryRuleMap(  );
		if(sRet.compare(SUCCESS)){
			THROW_BILL(SYS_ERR_CODE, "", sRet);
		}

		do{
			bool bHaveDataFlag = false;
			pBTL = new RuleMap_Info_t();
			if(!pBTL){
				THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR);
			}
			
			////取游标里面 1条记录;					
			sRet= nextRuleMap( pBTL ,bHaveDataFlag);
			if(sRet.compare(SUCCESS)){
				THROW_BILL(SYS_ERR_CODE, "", sRet);
			}
			
			////游标对应的记录全部取结束;				
			if( !bHaveDataFlag){
				break;
			}
				
			rvBTL.push_back(pBTL);
			pBTL = NULL;
			
		} while(1);


	} 
	catch(BillException &bex)
	{
		sRet = bex.getContext();
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );			
		sRet = SELECT_BALANCE_TRANSFER_ERROR;
	}
	catch(...)
	{
		sRet =  SELECT_BALANCE_TRANSFER_ERROR;
	}

	RELEASE_CLASS_INSTANCE(pBTL);
	
	if(sRet.compare(SUCCESS)){
		DELETE_VECT_CLEAR(rvBTL);
	}

	return sRet;
}
	
string C_bss_rule_map_prs::freshRuleMapInfo( vector<RuleMap_Info_t*>& rvBTL )
{
	string sRet = SUCCESS;
	long lRecCnt = 0;

	sRet = getRuleMapCnt( lRecCnt );
	if(!sRet.compare(SUCCESS) ){
		
		if( rvBTL.size() != lRecCnt ){
			//A_BSS_FUNDS_RULE_MAP发生变化,则重新取 ;
			DELETE_VECT_CLEAR(rvBTL);
			sRet = getRuleMapInfo( rvBTL);
		}		
	}

	return sRet;
}



string C_bss_rule_map_prs::insertBssFundsLog(const long& lPROD_2_PP_ID
		,const long& lRTP_NBR ,const long& lPaymentID,const long& lServID 
		,const long& lAcctID  ,const char* sAccNbr ,const char* sState
		,const string& strContext )
{
	string sRet =SUCCESS;
	string sSQL;
	int iCurPosition = 1;
	char sERR_CONT[200+1] = {0};
	///alter table  A_Bss_Funds_log add( ERR_CONT VARCHAR2(200) );  
	///考虑表字段长度限制 ,防止溢出;

	try
	{
		memset(sERR_CONT ,0 ,sizeof(sERR_CONT) );
		memcpy(sERR_CONT ,strContext.c_str() ,sizeof(sERR_CONT)-1 );
		sERR_CONT[ sizeof(sERR_CONT)-1 ] = 0 ;
		
		sSQL.clear();
		sSQL =  " insert into a_bss_funds_log( " 
		 "  FUNDS_LOG_ID " 
		 "   ,PROD_2_PP_ID " 
		 "   ,RTP_NBR " 
		 "   ,SERV_ID " 
		 "   ,ACCT_ID ,Payment_ID ,ERR_CONT" 
		 "   ,ACC_NBR " 
		 "   ,STATE " 
		 "   ,CREATED_DATE " 
		 "   ,STATE_DATE ) " 
		 " Values  ( " 			///" Select " 
		 	" FUNDS_LOG_ID_SEQ.nextval " 
		 "   , :PROD_2_PP_ID " 
		 "   , :RTP_NBR " 
		 "   , :SERV_ID " 
		 "   , :ACCT_ID ,:Payment_ID ,:ERR_CONT " 
		 "   , :ACC_NBR " 
		 "   , :STATE " 
		 "   , SYSDATE " 
		 "   , SYSDATE  ) "  		/// " " FROM dual "
			;

		newQuery();
		setSQL(sSQL);

		setParameter(iCurPosition++, lPROD_2_PP_ID);
		setParameter(iCurPosition++, lRTP_NBR);
		setParameter(iCurPosition++, lServID);		
		setParameter(iCurPosition++, lAcctID);
		setParameter(iCurPosition++, lPaymentID );
		setParameter(iCurPosition++, sERR_CONT );
		
		setParameter(iCurPosition++, sAccNbr );
		setParameter(iCurPosition++, sState);

		executeUpdate();
		close();
		
	}
	catch(SQLException &oraex)
	{
		userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
		return UPDATE_BALANCE_TRANSFER_ERROR;
	}
	catch(...)
	{
		return UPDATE_BALANCE_TRANSFER_ERROR;
	}

	return sRet;
}

string C_bss_rule_map_prs::insertAScanPayment(TJ_BssPay_Info_t* Bsspayinfo
		,const string& sScan_date, const string& sCash_date )
{
	string sRet =SUCCESS;
	string sSQL;
	int iCurPosition = 1;
	try
	{
		sSQL.clear();
		sSQL =  " insert into a_scan_payment( "
				"  SCAN_PAYMENT_ID,STAFF_ID,AREA_CODE,ACCT_ID,AMOUNT,CREATE_DATE "
				" ,SCAN_TYPE,SCAN_STATE, SCAN_DATE, SCAN_STATE_DATE, SCAN_CHANNEL"
				" ,SCAN_ERROR, CASH_STATE, CASH_DATE, CASH_PAYMENT_ID )"
				" Values  ( "
				":SCAN_PAYMENT_ID, 3,'022', :ACCT_ID, :AMOUNT, TO_DATE(:CREATE_DATE,'YYYYMMDDHH24MISS')"  //如何写日期插入，单引号？
				" ,'1', 'S0P', TO_DATE(:SCAN_DATE,'YYYYMMDDHH24MISS'), TO_DATE(:SCAN_STATE_DATE, 'YYYYMMDDHH24MISS'), '1' "
				" ,'0支付成功', 'C0P', TO_DATE(:CASH_DATE,'YYYYMMDDHH24MISS'), :CASH_PAYMENT_ID ) " ;

		newQuery();
		setSQL(sSQL);

		setParameter(iCurPosition++, Bsspayinfo->ScanpaymentID);
		setParameter(iCurPosition++, Bsspayinfo->lAcctID);
		setParameter(iCurPosition++, Bsspayinfo->lAmount);
		setParameter(iCurPosition++, sScan_date);
		setParameter(iCurPosition++, sScan_date);
		setParameter(iCurPosition++, sScan_date);
		setParameter(iCurPosition++, sCash_date);
		setParameter(iCurPosition++, Bsspayinfo->lPaymentID);

		executeUpdate();
		close();
	}
	catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__
					,  oraex.getErrorCode(), oraex.getMessage().c_str() );

			return INSERT_ASCAN_PAYMENT_FAIL;
		}
		catch(...)
		{
			return INSERT_ASCAN_PAYMENT_FAIL;
		}

		return sRet;

}

string C_bss_rule_map_prs::insertAPaymentExtent(const long& lPaymentID
	,const string& sCreate_date, TJ_BssPay_Info_t* Bsspayinfo )
{
	string sRet =SUCCESS;
	string sRet1;
	string sSQL;
	string sTablename = "";
	int iCurPosition = 1;
	int v_cur_time;
	C_bss_rule_map_prs  *pBssRuleMapPrs2 = NULL ;

	try
	{
		//sTablename = "A_PAYMENT_INFO_EXTEND_11812";
		
		sRet1 = pBssRuleMapPrs2->getTablename_Date(sTablename);
		
		if (sRet1.compare(SUCCESS)){
				userlog("lPaymentID(lPaymentID[%ld]) Failed!" 
					,lPaymentID);
				THROW_BILL(SYS_ERR_CODE, "" ,sRet1); 
			}
		
		sSQL.clear();
		sSQL =  " insert into "+sTablename+" ("
				"  PAYMENT_ID, BALANCE_TYPE_ID, PAY_ACCT_NAME, CHECK_NUM, CREATE_DATE "
				" , PAY_DATE, BANK_ACCT_TYPE, BANK_TYPE, BANK_ACCT_NUM, CRM_SERIES, MODIFY_DATE"
				" ,SOURCE_TYPE, OL_ID, OPERATION_TYPE ) "
				" Values  ( "
				":PAYMENT_ID, 6, :PAY_ACCT_NAME, :CHECK_NUM, :CREATE_DATE, :PAY_DATE, :BANK_ACCT_TYPE, :BANK_TYPE "
				" ,:BANK_ACCT_NUM, '269659432', :MODIFY_DATE, 1, '220142543791','5KB') " ;

		newQuery();
		setSQL(sSQL);

		setParameter(iCurPosition++, lPaymentID);
		setParameter(iCurPosition++, Bsspayinfo->PayacctName);
		setParameter(iCurPosition++, Bsspayinfo->CheckNum);
		setParameter(iCurPosition++, sCreate_date);
		setParameter(iCurPosition++, Bsspayinfo->Pay_date);
		setParameter(iCurPosition++, Bsspayinfo->Bankacct_type);
		setParameter(iCurPosition++, Bsspayinfo->Bank_type);
		setParameter(iCurPosition++, Bsspayinfo->BankacctNum);
		setParameter(iCurPosition++, sCreate_date);

		executeUpdate();
		close();
	}
	catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__
					,  oraex.getErrorCode(), oraex.getMessage().c_str() );

			return INSERT_APAYMENT_INFO_EXT_FAIL;
		}
		catch(...)
		{
			return INSERT_APAYMENT_INFO_EXT_FAIL;
		}

		return sRet;

}


string C_bss_rule_map_prs::getBssNbrSuccCnt(const long& lPROD_2_PP_ID ,long& rlFlag)
{
        string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;

		long lRecCnt = 0;
		
        try
        {
			sSQL.clear();
			sSQL  = " select count(1) " 
                	" from  a_bss_funds_log   " 
                	" WHERE   PROD_2_PP_ID = :PROD_2_PP_ID " 
                	"  AND STATE = :STATE " ;


				newQuery();
				setSQL(sSQL);
				
				setParameter(iCurPosition++ ,lPROD_2_PP_ID );
				setParameter(iCurPosition++ , "I0D" );
				
				executeQuery();
				if( m_rs->next() ){
					lRecCnt = (long)m_rs->getNumber(1);
				}
				
				closeResultSet();
				close();
				
			rlFlag = (lRecCnt>0)? 1:0;
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}

        return sRet;
}



string  C_bss_rule_map_prs::checkVirmProcessID(const long& rlProcessID
			,string& strExePathFile )
{

	string sSQL , sRet =MALLOC_ERROR_STR;
	long lProcessState = 999;
	int  iPosition = 1;

	strExePathFile = "-1";

	try
	{
			sSQL.clear();
			
			sSQL  =" SELECT Process_State ,EXE_Path_File "
				" from A_VIRM_Process "
				" where  Process_ID  = :rlProcessID " ;
			sSQL += " FOR  UPDATE  NOWAIT  " ;

			newQuery();
			setSQL( sSQL.c_str() );
			setParameter( 1,  rlProcessID);
			executeQuery();

			if( m_rs->next() ){
				lProcessState =  (long)m_rs->getNumber(1); 
				strExePathFile = m_rs->getString(2);
			}
			
			closeResultSet();
			close();
			
	 		userlog("Process_State[%ld] ,rlProcessID[%ld],strExePathFile[%s]" 
				,lProcessState ,rlProcessID ,strExePathFile.c_str() );
			
			if( lProcessState ){
				sRet = "Process_ID已经运行或者不存在" ;
			}
			else{
				sRet = SUCCESS;
				//##刷A_VIRM_Process为运行状态
				sSQL.clear();			
				sSQL  =" Update A_VIRM_Process "
					" SET  Process_State = 1 , State_DATE = SYSDATE "
							" ,CREATED_DATE = SYSDATE ,PID =:pid "
					" where  Process_ID  = :rlProcessID " ;

				newQuery();
				setSQL( sSQL.c_str() );

				iPosition = 1;
				setParameter( iPosition++,  (long)getpid() );
				setParameter( iPosition++,  rlProcessID);
				
				executeUpdate( );
				close();
				
				gbNormalRunFlag = true ;
				///o_ACCT_db.commit();				
			}

	}
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}


	return sRet;
}



string  C_bss_rule_map_prs::updateVirmProcessByID(const long& rlProcessID)
{

	string sSQL , sRet = SUCCESS ;

	try
	{

			sSQL  =" Update A_VIRM_Process "
				" SET  Process_State = 0 , State_DATE = SYSDATE "
				" where  Process_ID  = :rlProcessID " ;

			sSQL  += " AND Process_State = 1 " ;

			newQuery();
			setSQL( sSQL.c_str() );
			setParameter( 1,  rlProcessID);

			executeUpdate( );
			close();
			
		 sRet = SUCCESS ;
	}
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}
		catch(...)
		{
			sRet =  SELECT_BALANCE_TRANSFER_ERROR ;
		}


	return sRet;
}

string C_bss_rule_map_prs::IsServIDExist( const long& rlServID ,bool& rbExist )
{
      string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;

		long rlCnt = 0;
		
        try
        {
			sSQL.clear();
			sSQL  = " select count(1) " 
                	" from  SERV where SERV_ID = :SERV_ID" ;

				newQuery();
				setSQL(sSQL);
				
				setParameter(iCurPosition++ ,rlServID);
				executeQuery();
				if( m_rs->next() ){
					rlCnt = (long)m_rs->getNumber(1);
				}
				
				closeResultSet();
				close();				

			rbExist = ( rlCnt >=1 ? true :false );
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet = SELECT_SERV_INFO_ERROR;
		}
		catch(...)
		{
			sRet = SELECT_SERV_INFO_ERROR;
		}

        return sRet;
}


string C_bss_rule_map_prs::getTablename_Date(string& strTableName)
{
		string sRet = SUCCESS;
    string sSQL;
    int iCurPosition = 1 ;


        try
        {
			sSQL.clear();
			sSQL  = " select a.table_name ";
      		sSQL += " from  A_SPLIT_TABLE_CONFIG   a ";
      		sSQL += "   WHERE  a.table_type = 21 ";
      		sSQL += "   AND a.billing_cycle_id = 1 || to_char(sysdate, 'yymm')" ;    
      		sSQL += "   AND rownum < 2" ;


				newQuery(); //新建查询类
				setSQL(sSQL);

				executeQuery();
				if( m_rs->next() ){
					strTableName = m_rs->getString(1); //选择结果的第一列（为字符串）作为返回
				}

				closeResultSet();
				close();

        }
		catch(SQLException &oraex)
		{
			userlog("[%s]_[%d] : oraex.ErrorCode(%d),  ErrMsg[%s]" , __FILE__ , __LINE__,oraex.getErrorCode(), oraex.getMessage().c_str() );		 
			closeResultSet();
			close();
			sRet = oraex.getMessage();
		}
		catch(BillException &bex)
		{
			sRet = bex.getContext();	
		}
		catch(exception &ex)
		{
			sRet = ex.what();		
		}
		catch(...)
		{
			sRet = UNKNOW_EXCEPTION;		
		}	
		return sRet;

}


string C_bss_rule_map_prs::getDBSessionInfo(const char* sProgName ,string& sHostName 
	,long& lSID ,string& sLogonTime  )
{
      string sRet = SUCCESS;
        string sSQL;
        int iCurPosition = 1 ;
			long lSPID = -1;

		lSID = -1;
		sLogonTime = "-1" ;
		
      try
      {
					sSQL.clear();
					sSQL  = " select b.SID,to_char( b.LOGON_TIME,'yyyymmddhh24miss') "
								" ,to_number(a.spid) SPID  ,b.PROGRAM ,b.machine "
                " from v$process a,v$session b "
                " where a.addr=b.paddr "
                  " and b.PROCESS= TO_CHAR( :client_pid) " ;
		
					newQuery();
					setSQL(sSQL);
				
					setParameter(iCurPosition++ ,(long)getpid() );
					executeQuery();
				
				while( m_rs->next() ){
					iCurPosition=1;
					lSID = (long)m_rs->getNumber(iCurPosition++);
					sLogonTime =  m_rs->getString(iCurPosition++);
					lSPID = (long)m_rs->getNumber(iCurPosition++);

					string strProg = m_rs->getString(iCurPosition++);
					sHostName =  m_rs->getString(iCurPosition++);

					userlog("lSID[%ld],sLogonTime[%s],lSPID[%ld],sHostName[%s]"
						",ProgName[%s]"
						,lSID ,sLogonTime.c_str() ,lSPID,sHostName.c_str() 
						, strProg.c_str() );
				}
				
				closeResultSet();
				close();
			
			sRet = SUCCESS;
        }
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			
			sRet = SELECT_SERV_INFO_ERROR;
		}
		catch(...)
		{
			sRet = SELECT_SERV_INFO_ERROR;
		}

        return sRet;
}



string getSessionInfo( const char* sProgName ,string& sHostName 
	,long& lSID ,string& sLogonTime  )
{		
	long	lErrCode = 0 ;
	long lRet = 0;

	string sRet = MALLOC_ERROR_STR;
	string	sContext = "9999999999999xx" ;
	string	sErrMsg;	

	string   strExePathFile_cfg = "-1" ;

	C_bss_rule_map_prs  *pBssRuleMapPrs = NULL ;
	

	try
	{	
		
		pBssRuleMapPrs= new C_bss_rule_map_prs( gpACCT_DBLink);
		if( !pBssRuleMapPrs){
			 THROW_BILL(SYS_ERR_CODE, "new C_bss_rule_map_prs( )", MALLOC_ERROR_STR );
		}

		sRet =pBssRuleMapPrs->getDBSessionInfo(sProgName ,sHostName
												,lSID ,sLogonTime   );
		if (sRet.compare(SUCCESS) ){
			THROW_BILL(SYS_ERR_CODE,"",sRet);
		}


		sRet = SUCCESS;
	}
	catch(BillException &bex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = bex.getErrCode();
		sErrMsg = bex.getErrMsg();
		sRet = sContext = bex.getContext();
	}
	catch(exception &ex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = ex.what();
	}
	catch(...)
	{
		lRet = OTHER_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = "Other Error";
	}
	
	//###释放空间----;
	RELEASE_CLASS_INSTANCE( pBssRuleMapPrs );	

	return sRet;
}

string checkProcessIDRun ( const long rlProcessID ,const string& rstrPath )
{		
	long	lErrCode = 0 ;
	long lRet = 0;

	string sRet = MALLOC_ERROR_STR;
	string	sContext = "9999999999999xx" ;
	string	sErrMsg;	

	string   strExePathFile_cfg = "-1" ;

	C_bss_rule_map_prs  *pBssRuleMapPrs = NULL ;
	

	try
	{	
		
		pBssRuleMapPrs= new C_bss_rule_map_prs( gpACCT_DBLink);
		if( !pBssRuleMapPrs){
			 THROW_BILL(SYS_ERR_CODE, "new C_bss_rule_map_prs( )", MALLOC_ERROR_STR );
		}

		sRet =pBssRuleMapPrs->checkVirmProcessID( rlProcessID ,strExePathFile_cfg);
		if (sRet.compare(SUCCESS) ){
			THROW_BILL(SYS_ERR_CODE,"",sRet);
		}
		o_ACCT_db.commit();

		if( strExePathFile_cfg.compare(rstrPath) ){
			userlog("strExePathFile_cfg[%s] rstrPath[%s]不一致!" 
				,strExePathFile_cfg.c_str() ,rstrPath.c_str() );
			THROW_BILL(SYS_ERR_CODE ,"Exe_Path_File配置不对" ,"Exe_Path_File配置不对" );
		}

		sRet = SUCCESS;
	}
	catch(BillException &bex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = bex.getErrCode();
		sErrMsg = bex.getErrMsg();
		sRet = sContext = bex.getContext();
	}
	catch(exception &ex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = ex.what();
	}
	catch(...)
	{
		lRet = OTHER_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = "Other Error";
	}
	
	//###释放空间----;
	RELEASE_CLASS_INSTANCE( pBssRuleMapPrs );	

	return sRet;
}



string RestoreNormalState( const long rlProcessID   )
{		
	long	lErrCode = 0 ;
	long lRet = 0;

	string sRet = MALLOC_ERROR_STR;
	string	sContext = "9999999999999xx" ;
	string	sErrMsg;	

	C_bss_rule_map_prs  *pBssRuleMapPrs = NULL ;
	

	try
	{	
		
		pBssRuleMapPrs= new C_bss_rule_map_prs( gpACCT_DBLink);
		if( !pBssRuleMapPrs){
			 THROW_BILL(SYS_ERR_CODE, "new C_bss_rule_map_prs( )", MALLOC_ERROR_STR );
		}

		sRet =pBssRuleMapPrs->updateVirmProcessByID( rlProcessID );
		if (sRet.compare(SUCCESS) ){
			THROW_BILL(SYS_ERR_CODE,"",sRet);
		}

		sRet = SUCCESS;
	}
	catch(BillException &bex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = bex.getErrCode();
		sErrMsg = bex.getErrMsg();
		sRet = sContext = bex.getContext();
	}
	catch(exception &ex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = ex.what();
	}
	catch(...)
	{
		lRet = OTHER_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = "Other Error";
	}
	
	//###释放空间----;
	RELEASE_CLASS_INSTANCE( pBssRuleMapPrs );	

	return sRet;
}


int  My_getcwd(string& strPath )
{ 
	 char buf[1024]={0};
	 size_t len = 0;		
	char *p = NULL ;
	strPath = "" ;

	///##增加hostname; add@2009.03.02 night;
	if( !gethostname(buf, sizeof(buf)-1 ) ){
		buf[ sizeof(buf)-1] = 0 ;
		gstrHostName = buf ;
		printf("gethostname() ok ! buf[%s] \n" , buf );
	}
	else{
		printf( "gethostname() failed !\n" );
		return -2;	
	}
		
	strPath = buf;
	strPath += "#";
	//##end 增加hostname;------
	
	p = getcwd(buf, sizeof(buf)-1 );	
	buf[ sizeof(buf)-1] = 0 ;	
	if(p){
	  printf("getcwd ok ! buf[%s] \n" , buf );
	  len = strlen( buf );	  
	  printf("buf[len[%d]-1] =%c \n" ,len  ,buf[len-1] );
	  
	  strPath += buf;
	  if( '/' != buf[len-1] ){
	  	strPath += "/" ;
	  }
	  
	}
	else{
	 printf("getcwd Failed! \n" );
	 return -1;
	}
	
	return 0;
}


/*------------------------------------------*/
static long  GetProcessID_lc()
{
        char* env = NULL ;

        if( (env=getenv("PROCESS_ID") ) !=NULL ){
        	printf("PROCESS_ID :%d\n", atol(env) );
        	return  atol(env);
        }
        else{
        	cout << "[ERROR]: getenv(PROCESS_ID) == NULL, 获取环境变量PROCESS_ID失败" <<endl;
        	return -1;
        }
}


///////////////////////////////////////////////////////////

/*时钟信号相应程序*/
void AlarmProcess_Bss(int sig)
{
    /*时钟信号相应*/
	userlog("SigProcess(%ld)\n", sig);
    return ;
}


/*信号处理程序sig为信号*/
static void SigProcess_Bss(int sig)
{
    /*信号处理*/
    userlog("catch SigProcess(%ld), will DELETE_VECT( gvRuleMapInfo_vt.size=%u )..."
    	, sig ,gvRuleMapInfo_vt.size() );
	
	DELETE_VECT_CLEAR( gvRuleMapInfo_vt );

	//如果已经连接则关闭;
	if(gpBSS_DBLink){
		o_BSS_db.rollback();
		o_BSS_db.disconnect();
		gpBSS_DBLink = NULL;
	}

	if(gpACCT_DBLink){
		///进程被Killed时,回滚所有DB事务;
		o_ACCT_db.rollback();	

		///进程控制表,对应进程由'运行态' 变更成 '停止态'; add@2009.03.02;
		if( gbNormalRunFlag ){
			RestoreNormalState(glProcessID );
			o_ACCT_db.commit();	
		}		
		C_SEARCH_ENGINE_PROCESS::Instance()->Release();
		o_ACCT_db.disconnect();
		gpACCT_DBLink = NULL;
	}

    userlog("SigProcess(%ld):gvRuleMapInfo_vt.size()=%u; ending ... "
		, sig ,gvRuleMapInfo_vt.size());
	
    exit(0);
}


//##对外BSS_CRM等使用; CRM充值;  add@2009.02.26;
int   main(int argc, char *argv[])
{

	long lRet = NO_ERROR;
	long	lErrCode = 0 ;
	long lBssPayCnt = 0;	
	long lRecLimit = 100;
	int kk = 1;
	int iIdle_sleepTicks = 30;
	long lDynamic = 0;
	long lCtrlFlag = 0;
	
	char sServerName[100], sUserName[20], sPassword[50], sConnStr[20];
	bool bReConnFlag = true;
	string sRet = MALLOC_ERROR_STR ;
	string	sContext = "9999999999999xx" ;
	string	sErrMsg="noError";
		string  strPath = ".";
	///long 	lProcessID = 0;

	try{
		
		gvRuleMapInfo_vt.clear();
		/*设置信号处理*/
		sigset(SIGINT , SigProcess_Bss);
		sigset(SIGTERM, SigProcess_Bss);
		sigset(SIGABRT, SIG_IGN);
		sigset(SIGQUIT, SigProcess_Bss);
		sigset(SIGHUP , SIG_IGN);
		sigset(SIGALRM, AlarmProcess_Bss);

		
		if(argc <= 1){
			sErrMsg = "Usage: " ;
			sErrMsg += argv[0] ;
			sErrMsg += " STAFF_ID [iIdle_sleepTicks [每次取Bss.Rent记录数] ]" ;	
			sErrMsg += " [ glPROD_2_PP_ID glServ_ID lCtrlFlag ]" ;
			cout << sErrMsg << endl;		

			THROW_BILL(SYS_ERR_CODE, sErrMsg, MALLOC_ERROR_STR);
		}

		glMOBILE_PROXY_STAFF_ID = atol( argv[kk++] );
		if(argc > kk) iIdle_sleepTicks	= atoi( argv[kk++] );
		if(argc > kk) lRecLimit	= atol( argv[kk++] );
		if(argc > kk) glPROD_2_PP_ID = atol( argv[kk++] );
		if(argc > kk) glServID = atol( argv[kk++] );

		if(argc > kk){ 
			lCtrlFlag = atol( argv[kk++] );
			if(lCtrlFlag <= 0){
				lCtrlFlag = 0X10;
			}
		}
		
		userlog("lCtrlFlag=%ld " ,lCtrlFlag );
		
		if( glPROD_2_PP_ID <= 0 ) glPROD_2_PP_ID = -1;
		if( glServID <= 0 ) glServID = -1;

		
		if(glMOBILE_PROXY_STAFF_ID <= 0 ){
			sErrMsg = "工号不合法!" ;		
			THROW_BILL(SYS_ERR_CODE, sErrMsg, MALLOC_ERROR_STR);
		}

		if(iIdle_sleepTicks <= 0){
			iIdle_sleepTicks = 30;
		}

		if( lRecLimit <= 0){
			 lRecLimit = 100;
		}

		userlog("STAFF_ID[%ld] ,iIdle_sleepTicks[%d] ,lRecLimit[%ld] " 
			" ,glPROD_2_PP_ID[%ld] , glServID[%ld] "
			, glMOBILE_PROXY_STAFF_ID ,iIdle_sleepTicks ,lRecLimit 
 			,glPROD_2_PP_ID , glServID );

		glProcessID = GetProcessID_lc();
		if(glProcessID <= 0 ){
			THROW_BILL(SYS_ERR_CODE, "GetProcessID_lc" ,MALLOC_ERROR_STR);
		}

		if( My_getcwd(strPath) ){
			THROW_BILL(SYS_ERR_CODE, "My_getcwd" ,MALLOC_ERROR_STR);
		}
		strPath += argv[0] ;

		sErrMsg="noError";
	}
	catch(BillException &bex)
	{
			lRet = SYS_ERR_CODE;
			lErrCode = bex.getErrCode();
			sErrMsg = bex.getErrMsg();
			sRet = sContext = bex.getContext();
	}
	catch(...)
	{
			lRet = OTHER_ERR_CODE;
			lErrCode = lRet ;
			sErrMsg = "Other Error";
	}
	
	if(lRet){
		userlog( "%s 初始化报错: sErrMsg[%s]" ,argv[0] ,sErrMsg.c_str() );	
		SigProcess_Bss(-123456);
	}

	char sDateTime_Pre[16]={0};
	C_Date oNowDate;
	oNowDate.getCurDate();
	strcpy( sDateTime_Pre ,oNowDate.toString() );
	userlog("oNowDate[%s],sDateTime_Pre[%s]; while(1) begin... " 
		,oNowDate.toString(),sDateTime_Pre );
	
	while(1)
	{
	
		try
		{
		
			//--0,判断是否要重新连接数据库;
			//add@2009.03.05;
			if(lCtrlFlag & 0X01){
				oNowDate.getCurDate();
				if( memcmp( oNowDate.toString(),sDateTime_Pre,8) >0 ){
					userlog("oNowDate[%s],sDateTime_Pre[%s] NewDay come!" 
						,oNowDate.toString(),sDateTime_Pre );
					///新的一天到了,Reconnect DB;
					bReConnFlag = true; 
					strcpy( sDateTime_Pre ,oNowDate.toString() );					
				}
			}
			
			if(bReConnFlag){

				//如果已经连接则关闭;
				if(gpBSS_DBLink){
					o_BSS_db.disconnect();
					gpBSS_DBLink = NULL;
				}
				if(gpACCT_DBLink){
					C_SEARCH_ENGINE_PROCESS::Instance()->Release();
					o_ACCT_db.disconnect();
					gpACCT_DBLink = NULL;
				}
				
				///##连接BSS_DB;  tj_bss_funds=INTF_BILL/passwd/tj_CRM_intf
				//strcpy(sServerName,"tj_bss_funds_test_2");
				strcpy(sServerName,"tj_bss_funds2");
				lRet = GetConnectInfo(sServerName, sConnStr, sUserName, sPassword);
				if ( lRet){
					userlog("<%s>在<NONXA_SERVERS.ini>无配置!" ,sServerName);
					 SigProcess_Bss(-1);
				}
				
				userlog("====o_BSS_db<%s>连接数据库[%s/passwd@%s]...." 
						,sServerName,sUserName, sConnStr);

				gpBSS_DBLink = o_BSS_db.connect(sUserName, sPassword, sConnStr);
				if(!gpBSS_DBLink){
					userlog("====<%s>连接数据库失败==[%s/%s@%s]==" 
						,sServerName,sUserName, sPassword, sConnStr);
					 SigProcess_Bss(-2);
				}
				
				///##连接ACCT_DB;
				strcpy(sServerName ,"CASH_BILL");
				lRet = GetConnectInfo(sServerName, sConnStr, sUserName, sPassword);
				if ( lRet){
					userlog("<%s>在<NONXA_SERVERS.ini>无配置!" ,sServerName);
					SigProcess_Bss(-3);
				}

				userlog("====o_ACCT_db<%s>连接数据库[%s/passwd@%s]...." 
						,sServerName,sUserName, sConnStr);

				gpACCT_DBLink = o_ACCT_db.connect(sUserName, sPassword, sConnStr);
				if(!gpACCT_DBLink){
					userlog("====<%s>连接数据库失败==[%s/%s@%s]==" 
						,sServerName,sUserName, sPassword, sConnStr);
					SigProcess_Bss(-4);
				}
				
				///add@2009.02.27; 
				///C_AGENT_BILL_PROCESS_JS.cpp很多直接写的是gpDBLink :-)
				gpDBLink = gpACCT_DBLink;

				C_SEARCH_ENGINE_PROCESS::Instance()->setDBConn(gpDBLink);
				C_SEARCH_ENGINE_PROCESS::Instance()->initParam();

				//##lProcessID是否已经在运行;	add@2009.03.02;
				if( !gbNormalRunFlag ){
					sRet = checkProcessIDRun ( glProcessID ,strPath );
					if(sRet.compare(SUCCESS)){
						userlog( "checkProcessIDRun(glProcessID[%ld],strPath[%s])=%s" 
							,glProcessID ,strPath.c_str() ,sRet.c_str() );
						SigProcess_Bss( -5);
					}
				}
				else{
					userlog("gbNormalRunFlag[true] ,PID[%u]" ,getpid() );
				}

				///如何获取DataBase Session ID; v$session; to be add;
				/// 测试成功@2009.03.06;
				if(1 && (lCtrlFlag & 0X02) ){
					long lSID=0;
					string sLogonTime ="";
					char *p = argv[0];
					
					while(p && ('.'== *p || '/'== *p) ){
						p++;
					}
					///因为有些客户端在v$session的PROGRAM字段里面最长只有14 ;
					char sProgName[14+1] = {0};
					memset(sProgName,0,sizeof(sProgName) );
					memcpy(sProgName ,p ,sizeof(sProgName)-1 );
					
					sRet= getSessionInfo( sProgName ,gstrHostName,lSID ,sLogonTime);
					if(sRet.compare(SUCCESS) ){
						userlog("prog[%s],sHost[%s],lSID[%ld],sLogonTime[%s] fail!"  
							,sProgName,gstrHostName.c_str() ,lSID ,sLogonTime.c_str());
						sRet = SUCCESS;
					}

					userlog("(prog[%s],sHostName[%s],lSID[%ld],sLogonTime[%s]) OK!"  
						,sProgName ,gstrHostName.c_str() ,lSID ,sLogonTime.c_str() );
				}	

				bReConnFlag = false;
			}

			
			///<1>开始做业务;
			/*
			 -------------------------------------------------------------------
			 流程: 
			  1、从Rent@bss_db取出(BSS_pay_plan_ID，CHARGE ，号码或者ServID);
			  2、根据支付计划标识到A_BSS_FUNDS_RULE_MAP 取出活动对应的预存规则
			  		(是否赠送，是否分期返还;
			  3、根据预存规则把预存款送到 Acct_Balance@tj_c_acct；
			  -------------------------------------------------------------------
			*/
			lBssPayCnt = 0;
			if( !(lCtrlFlag & 0X10) ){

				sRet = BSS_FUNDS( lRecLimit ,lBssPayCnt ,lDynamic );
				if(sRet.compare(SUCCESS)){
					THROW_BILL(SYS_ERR_CODE, "", sRet);				
				}
			}
			else{
				userlog("skip BSS_FUNDS... " );
			}
		}
		catch(SQLException &oraex)
		{
			userlog( "[%s]_[%d] : oraex.ErrorCode(%d),	ErrMsg[%s]" , __FILE__ , __LINE__ 
				,  oraex.getErrorCode(), oraex.getMessage().c_str() );
			lRet = SYS_ERR_CODE;
			sRet =  oraex.getMessage()  ;
		}
		catch(BillException &bex)
		{
			lRet = SYS_ERR_CODE;
			lErrCode = bex.getErrCode();
			sErrMsg = bex.getErrMsg();
			sRet = sContext = bex.getContext();
		}
		catch(exception &ex)
		{
			lRet = SYS_ERR_CODE;
			lErrCode = lRet ;
			sErrMsg = ex.what();
		}
		catch(...)
		{
			lRet = OTHER_ERR_CODE;
			lErrCode = lRet ;
			sErrMsg = "Other Error";
		}
		
		if( NO_ERROR != lRet )
		{
			C_ERRMSG_PROCESS*  pErrMsgProcess = new C_ERRMSG_PROCESS(gpACCT_DBLink);
			string sErrCodeMsg;
			pErrMsgProcess->getErrMsg(sContext, sErrCodeMsg);
			delete pErrMsgProcess;
			pErrMsgProcess = NULL;
			
			userlog( "错误信息:%s.SQL代码:%ld. CodeMsg[%s].sContext[%s]\n"
				,sErrMsg.c_str(), lErrCode, sErrCodeMsg.c_str() , sContext.c_str() );

			sleep( iIdle_sleepTicks *2 );
		}
		
		if( 0 == lBssPayCnt ){ ///Bss充值接口表无数据;
			sleep( iIdle_sleepTicks);
		}

		lRecLimit += lDynamic;
		if(lRecLimit < 10){
			lRecLimit = 10;
		}
		if(lRecLimit > 2000 ){
			lRecLimit = 2000 ;
		}
		
	}//while(1)

	SigProcess_Bss(0);
}


///===扫描Rent@bss_db取出(BSS_pay_plan_ID，CHARGE ，号码或者ServID);
string   BSS_FUNDS( const long& rlRecLimit ,long& rlBssPayCnt ,long& rlDynamic )
{
	string sRet = MALLOC_ERROR_STR;		
	string	sContext = "9999999999999xx" ;
	string	sErrMsg;
	string  strBssState = TJ_BSS_RENT_STATE_DEALED ;
	string  sLogState = "I0" ;
	long lRet = 0;	
	long	lErrCode = 0 ;
	///本次真正处理的记录数;
	long lBssPayCnt_Real =0 ;
	
	int j = 0 ,i=0 ;

	////---辅助指针,不分配内存;
	TJ_BssPay_Info_t* pBssPayInfo =NULL;
	//////////
	vector<TJ_BssPay_Info_t*> vBTL ;
	
	////---component.PROCESS----/////
	C_TJ_BSS_Funds_mgr	*pMobileTransMgr = NULL;	
	C_ERRMSG_PROCESS	*pErrMsgProcess = NULL;
	C_bss_rule_map_prs  *pBssRuleMapPrs = NULL ;
	//////////
			
	rlBssPayCnt = 0 ;
	rlDynamic = 0;

	try
	{	vBTL.clear();

		////---component.PROCESS----/////
		//modify begin by xlfei on 20170421 for增加未激活用户返赠款的处理,查rent表时要关联帐务库的表，将数据库链接从gpBSS_DBLink改为gpACCT_DBLink
		pMobileTransMgr = new  C_TJ_BSS_Funds_mgr(gpACCT_DBLink) ;
		//modify end by xlfei on 20170421 for增加未激活用户返赠款的处理,查rent表时要关联帐务库的表，将数据库链接从gpBSS_DBLink改为gpACCT_DBLink
		if ( !pMobileTransMgr ){
			THROW_BILL(SYS_ERR_CODE, "new C_TJ_BSS_Funds_mgr( )",MALLOC_ERROR_STR);
		}
		
		pBssRuleMapPrs= new C_bss_rule_map_prs(gpACCT_DBLink);
		if( !pBssRuleMapPrs){
			 THROW_BILL(SYS_ERR_CODE, "new C_bss_rule_map_prs( )", MALLOC_ERROR_STR );
		}

		sRet= pMobileTransMgr->getBssRentPayInfo(rlRecLimit ,vBTL );		
		if(sRet.compare(SUCCESS)){
			THROW_BILL(SYS_ERR_CODE, "", sRet);
		}
		
#ifdef DEBUG
		userlog("BssRentPay.size()=%u ,rlRecLimit[%ld]" ,vBTL.size() ,rlRecLimit );
#endif
		rlBssPayCnt = vBTL.size() ;
		if(vBTL.size() == rlRecLimit){
			rlDynamic = 1;
		}

		for(j=0; j< vBTL.size(); j++)
		{
			AScanPay_Info_t *pScanPayInfo =NULL;
			Apayment_Info_ext_t *pPaymentextInfo =NULL;
			
			bool bExist = false;
			
			pBssPayInfo = vBTL[j];
			sLogState = "I0" ;
			lRet = 0;

			//##Serv_ID对应的资料进计费系统有延迟的 先跳过; add@2009.03.04;
			sRet = pBssRuleMapPrs->IsServIDExist( pBssPayInfo->lServID ,bExist );
			if(sRet.compare(SUCCESS))
			{
				THROW_BILL(SYS_ERR_CODE, "IsServIDExist", sRet);				
			}
			
			if(!bExist)
			{
				userlog("SERV.SERV_ID[%ld] NOT_EXIST" ,pBssPayInfo->lServID );
				continue;
			}
			
			lBssPayCnt_Real++;

			///给Rent@bss_db加行锁 ;
			//select 1 from Rent where RENT_Serial_Nbr= :NBR  FOR update nowait;
			sRet = pMobileTransMgr->LockRentRow( pBssPayInfo->m_lPROD_2_PP_ID);
			if(sRet.compare(SUCCESS)){
				o_BSS_db.rollback();
				userlog("LockRentRow(PROD_2_PP_ID[%ld]) Failed!" 
					,pBssPayInfo->m_lPROD_2_PP_ID);
				THROW_BILL(SYS_ERR_CODE, "", sRet);				
			}

			sContext = "no_error" ;
			sRet = bss_funds_inter_One( *pBssPayInfo ,sContext);
			if(sRet.compare(SUCCESS)){
				o_ACCT_db.rollback();
				///取失败信息;to be add
				///
				strBssState = TJ_BSS_RENT_STATE_Failed;
			}
			else{
				strBssState = TJ_BSS_RENT_STATE_DEALED;
			}			

			///在计费系统增加交易日志; add@2009.02.28;
			sLogState += strBssState ;
			sRet = pBssRuleMapPrs->insertBssFundsLog( pBssPayInfo->m_lPROD_2_PP_ID
						,pBssPayInfo->lRTP_NBR ,pBssPayInfo->lPaymentID
						,pBssPayInfo->lServID  ,pBssPayInfo->lAcctID 
						,pBssPayInfo->sAccNbr ,sLogState.c_str() 
						,sContext );
			if (sRet.compare(SUCCESS)){
				userlog("insertBssFundsLog(PROD_2_PP_ID[%ld]) Failed!" 
					,pBssPayInfo->m_lPROD_2_PP_ID);
				THROW_BILL(SYS_ERR_CODE, "" ,sRet); 
			}
			
			strcpy(pScanPayInfo->Scan_date, "2019-02-25");  //采用系统时间
			strcpy(pScanPayInfo->Cash_date, "2019-02-27");		//采用系统时间	
			//pScanPayInfo->CashpaymentID= 9878480;
			//add to insert into A_SCAN_PAYMENT table
			sRet = pBssRuleMapPrs->insertAScanPayment(pBssPayInfo, pScanPayInfo->Scan_date
					,pScanPayInfo->Cash_date );
			if (sRet.compare(SUCCESS)){
				userlog("insertAScanPayment(PROD_2_PP_ID[%ld]) Failed!"
					,pBssPayInfo->m_lPROD_2_PP_ID);
				THROW_BILL(SYS_ERR_CODE, "" ,sRet);
			}
/*   这里不用该函数向表里添加数据了			
			strcpy(pPaymentextInfo->Create_date,"2019-02-25");  //通过某函数调用获得前端需要插入表的月份
			strcpy(pPaymentextInfo->Pay_date,"2019-02-25");
			pPaymentextInfo->PaymentID= 9878480;
			sRet = pBssRuleMapPrs->insertAPaymentExtent(pPaymentextInfo->PaymentID
						,pPaymentextInfo->Create_date, pBssPayInfo);
			if (sRet.compare(SUCCESS)){
				userlog("insertAPaymentExtent(PROD_2_PP_ID[%ld]) Failed!"
					,pBssPayInfo->m_lPROD_2_PP_ID);
				THROW_BILL(SYS_ERR_CODE, "" ,sRet);
						}
*/			
			///修改Rent@bss_db;
			sRet = pMobileTransMgr->updateBssRentPay(pBssPayInfo->m_lPROD_2_PP_ID ,strBssState );
			if (sRet.compare(SUCCESS)){
				userlog("updateBssRentPay(PROD_2_PP_ID[%ld]) Failed!" 
					,pBssPayInfo->m_lPROD_2_PP_ID);
				THROW_BILL(SYS_ERR_CODE, "" ,sRet); 
			}

			o_ACCT_db.commit();
			o_BSS_db.commit();			
		}
		
	}
	catch(BillException &bex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = bex.getErrCode();
		sErrMsg = bex.getErrMsg();
		sRet = sContext = bex.getContext();
	}
	catch(exception &ex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = ex.what();
	}
	catch(...)
	{
		lRet = OTHER_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = "Other Error";
	}

	if(lRet){
		o_ACCT_db.rollback();
		o_BSS_db.rollback();	
	}

	

	//###释放空间----;

	//////------component---
	RELEASE_CLASS_INSTANCE( pMobileTransMgr );
	RELEASE_CLASS_INSTANCE( pBssRuleMapPrs );

	///////---vector----
	DELETE_VECT_CLEAR( vBTL);

	if( 0==lBssPayCnt_Real && rlBssPayCnt > 0 && rlBssPayCnt >= rlRecLimit ){
		rlDynamic++;
	}
	if( lBssPayCnt_Real >= 1 && rlRecLimit >= 1000 ){
		rlDynamic = -1;
	}
	
	rlBssPayCnt = lBssPayCnt_Real ;

	return sRet;	
}


///===处理1条BSS充值请求;
string   bss_funds_inter_One( TJ_BssPay_Info_t& tTBssPayInfo ,string  &rsContext )
{

	long lRet = NO_ERROR;
	long lPaymentID = -1;
	char sRltErrMsg[SIZE_ERROR_MSG]={0};	

	string sRet = MALLOC_ERROR_STR;
	string sContext = "9999999999999xx" ;
	string sErrMsg;	

	long lErrCode = 0 ;
	long lAcctID = -1 ; 
	long lServID=-1;
	long lStaffID = glMOBILE_PROXY_STAFF_ID  ;
	long lTransferAmount = 0;
	long lFlag = 0;	
	int j = 0 ,i=0 ;
	int iReturnFlag = 0;
	
	char sAccNbr_SRC[SIZE_ACC_NBR] = {0}  ;
	char sFormat[32]= JSLIB_DATE_TIME_FORMAT;
	char sNowDateTime[32]={0};

	C_Date oDate;
				
	////---BO----/////
	T_SERVINFOQUERY  *pServInfoQuery = NULL;
	class T_DEPOSITINFO *pDepositInfo = NULL;
	class T_RETURNRULE *pReturnRule = NULL;
	class T_CASHBILL *pCashBill = NULL;

	////---辅助指针,不分配内存;

	//////////

	////---component.PROCESS----/////
	AGENT_BILL_MGR_JS	*pAgentBillMgr = NULL;	
	C_ERRMSG_PROCESS	*pErrMsgProcess = NULL;

	C_bss_rule_map_prs  *pBssRuleMapPrs = NULL ;
	C_ACCT_BALANCE_MGR *m_AcctBalanceMgr = NULL;
	  C_DEPOSIT_RETURN *m_DepositReturn = NULL ;
	//////////
	

	try
	{	
#ifdef _ABM
		//翼支付返还不进帐务余额, 无需生成ABM事物日志
		if (1L != tTBssPayInfo.m_lKBFlag)
		{
			//事务处理记录生成
			sRet.clear();
			sRet = startAbmTransaction(gpACCT_DBLink);
			if(sRet.compare(SUCCESS))
				THROW_BILL(SYS_ERR_CODE, "" ,sRet);
		}
#endif				
		pBssRuleMapPrs= new C_bss_rule_map_prs( gpACCT_DBLink);
		if( !pBssRuleMapPrs){
			 THROW_BILL(SYS_ERR_CODE, "new C_bss_rule_map_prs( )", MALLOC_ERROR_STR );
		}

		///a_bss_funds_log里面对应的PROD_2_PP_ID已有成功记录则报错;add@2009.02.28;
		sRet = pBssRuleMapPrs->getBssNbrSuccCnt(tTBssPayInfo.m_lPROD_2_PP_ID ,lFlag);
		if (sRet.compare(SUCCESS) ){
			THROW_BILL(SYS_ERR_CODE,"",sRet);
		}
		
		if(lFlag >= 1){			 
			THROW_BILL(SYS_ERR_CODE,"A_BSS_FUNDS_LOG有成功PROD_2_PP_ID" 
				,BSS_FUNDS_LOG_SUCC_EXIST);
		}

		///检查A_BSS_FUNDS_RULE_MAP配置是否发生变化;
		sRet = pBssRuleMapPrs->freshRuleMapInfo( gvRuleMapInfo_vt);
		if (sRet.compare(SUCCESS) ){
			THROW_BILL(SYS_ERR_CODE,"",sRet);
		}

#ifdef DEBUG
		userlog("gvRuleMapInfo_vt.size()=%u" ,gvRuleMapInfo_vt.size() );
#endif

		///使用BSS.Pay_Plan_ID来匹配计费系统的规则 ;
		bool bFounded = false;
		int iMatchCnt = 0;
		for(j=0; j< gvRuleMapInfo_vt.size(); j++){

			if(tTBssPayInfo.lPayPlanID ==gvRuleMapInfo_vt[j]->lBSS_PAY_PLAN_ID )
			{
				bFounded = true;
				i = j;
				iMatchCnt++;
#ifdef DEBUG		
				userlog("Rule[%d]=RTP_NBR[%ld]" ,j ,gvRuleMapInfo_vt[j]->lRTP_NBR );
#endif				
				//break;
			}
		}

		if(!bFounded){
			userlog( "BSS_TJ_PAY_PLAN_NOTFOUND BSS.PAY_PLAN_ID[%ld]" ,tTBssPayInfo.lPayPlanID );
			THROW_BILL(SYS_ERR_CODE,"找不到匹配的支付计划" ,BSS_TJ_PAY_PLAN_NOTFOUND);
		}

		if( iMatchCnt >= 2 ){
			THROW_BILL(SYS_ERR_CODE,"重复的支付计划" ,BSS_TJ_PAY_PLAN_NOTFOUND);
		}


		RuleMap_Info_t tRuleMapInfo= *gvRuleMapInfo_vt[i];
#ifdef DEBUG		
		userlog("lBSS_PAY_PLAN_ID=%ld, lBALANCE_TYPE_ID=%ld, lCHARGE=%ld"
			,tRuleMapInfo.lBSS_PAY_PLAN_ID 
			, tRuleMapInfo.lBALANCE_TYPE_ID 
			,tRuleMapInfo.lCHARGE);
#endif
		tTBssPayInfo.lRTP_NBR = tRuleMapInfo.lRTP_NBR;

		////---BO----/////
		pServInfoQuery	= new T_SERVINFOQUERY();
		if( !pServInfoQuery ){
			THROW_BILL(SYS_ERR_CODE,"new T_SERVINFOQUERY()", MALLOC_ERROR_STR );
		}
		
		////---component.PROCESS----/////
		pAgentBillMgr = new AGENT_BILL_MGR_JS( gpACCT_DBLink);
		if( !pAgentBillMgr ){
			 THROW_BILL(SYS_ERR_CODE, "new AGENT_BILL_MGR_JS( )", MALLOC_ERROR_STR );
		}
		
		m_AcctBalanceMgr = new C_ACCT_BALANCE_MGR(gpACCT_DBLink) ;
		if(!m_AcctBalanceMgr){
			 THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
		}
		
		m_DepositReturn = new C_DEPOSIT_RETURN( gpACCT_DBLink );
		if(!m_DepositReturn){
			 THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
		}

		
		//##add@2007.04.12
		if(1){
			pServInfoQuery->setQueryvalue1( Number2cSTR(tTBssPayInfo.lServID) );
			pServInfoQuery->setQueryflag( SERV_SERV_ID_QRY_FLAG) ;
			sRet.clear();
			//pServInfoQuery->getQueryvalue4( sRet );

			vector<string> &rvsState = pServInfoQuery->getQueryvalue3();
			rvsState.clear();
			rvsState.push_back(SERV_VALID_STATE);
			rvsState.push_back(SERV_USER_REQUEST_STOP_STATE);
			rvsState.push_back(SERV_STOP_STATE);
			rvsState.push_back(SERV_2HC_2HD_STATE);
			rvsState.push_back(SERV_OUT_DISABLE);
			//add begin by xlfei on 20170330 for紧急需求,57925	酷视卡首月优惠（赠送10元话费）要求未激活也能返赠款	
			//modify begin by xlfei on 20170420 for 增加未激活用户返赠款的处理
			bool bExist = false;
			pBssRuleMapPrs->jugde_2hn_deposit_offer(tTBssPayInfo.lPayPlanID, bExist);		
			if(bExist)
				{
					rvsState.push_back(SERV_STATE_PREP);
				}		
			//modify begin by xlfei on 20170420 for 增加未激活用户返赠款的处理		
			//add end by xlfei on 20170330 for紧急需求,57925	酷视卡首月优惠（赠送10元话费）要求未激活也能返赠款
			sRet.clear();	
			sRet = pAgentBillMgr->getServInfo( pServInfoQuery);
			if (sRet.compare(SUCCESS) ){
				THROW_BILL(SYS_ERR_CODE,"getServInfo()",sRet);
			}

			vector<T_SERVINFO*> &rvServInfo = pServInfoQuery->getServinfo();

#ifdef DEBUG
			userlog( "M_BAL_TRANS:src rvServInfo.size()=%ld " ,rvServInfo.size() );
#endif
			if (rvServInfo.size() <= 0){
				THROW_BILL(SYS_ERR_CODE, "" , SELECT_SERV_INFO_ERROR ); 			
			}

			if (rvServInfo.size() >= 3){
				THROW_BILL(SYS_ERR_CODE, "号码对应多个产品,建议到电信营业厅去充值;" 
					, SELECT_SERV_INFO_ERROR ); 			
			}

			lServID =  rvServInfo[0]->getServid();
			strcpy(sAccNbr_SRC , rvServInfo[0]->getAccnbr().c_str() );
			
			if( (long)rvServInfo[0]->getServid() != tTBssPayInfo.lServID ){
				THROW_BILL(SYS_ERR_CODE, "" ,BSS_TJ_SERV_ID_NOT_MATCH);
			}
			
			sRet = pAgentBillMgr->getAcctIDbyServID(lAcctID ,lServID,true );
			if(sRet.compare(SUCCESS)){
				throw BillException(SYS_ERR_CODE, "getAcctIDbyServID", sRet);
			}
			
		}
		
		AllTrim(sAccNbr_SRC);
#ifdef DEBUG
		userlog( ": sAccNbr_SRC[%s],lServID_src[%ld],lAcctID_src[%ld]" 
				,sAccNbr_SRC ,lServID ,lAcctID);		
#endif
		strcpy(tTBssPayInfo.sAccNbr ,sAccNbr_SRC );
		tTBssPayInfo.lAcctID = lAcctID;
		tTBssPayInfo.lRTP_NBR = tRuleMapInfo.lRTP_NBR;


		/////-------调用[ ]函数 (C_ACCT_BALANCE_MGR.cpp) ;
		pDepositInfo = new T_DEPOSITINFO;
		if ( ! pDepositInfo){
			THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
		}

		pDepositInfo->getReturnrule().clear();
		pDepositInfo->getReturnobject().clear();
		//##增加赠送条件判断@2007.04.18
		vector< T_CONFERVALUE* > &rvDepositConferValue  = pDepositInfo->getConfervalue();
		rvDepositConferValue.clear();
		
		pDepositInfo->setAcctid(lAcctID);
		pDepositInfo->setServid( lServID);
		pDepositInfo->setAccnbr(sAccNbr_SRC);
		pDepositInfo->setStaffid( lStaffID);
		pDepositInfo->setPaymethod(CASH_PAYMENT_METHOD); //11
		pDepositInfo->setPayedmethod(CASH_PAYMENT_METHOD); //11
		pDepositInfo->setSourcetype(BALANCE_SOURCE_CRM_SOURCE_TYPE); //'5VF'
		pDepositInfo->setSourcedesc("BssDeposit");
		pDepositInfo->setPaymentopertype(PAYMENT_DEPOSIT_OPERATION_TYPE); //'5KB'
		pDepositInfo->setOperationtype(BALANCE_SOURCE_DEPOSIT_OPERATION_TYPE); //'5UA'
		pDepositInfo->setInvoiceflag(INVOICE_OFFER_YES); //'IOY'
		pDepositInfo->setOperatorflag(DEPOSIT_OBJECT_TYPE_JUDGE); 
		pDepositInfo->setBalancetypeid(tRuleMapInfo.lBALANCE_TYPE_ID );

		//添加payment_info_xxx表的信息
		pDepositInfo->setPayacctname(tTBssPayInfo.PayacctName);  //PayAcctName，是固定值吗，还是传入参数？
		pDepositInfo->setCheckdate(tTBssPayInfo.Pay_date);  //CheckDate？？？哪里的？
		pDepositInfo->setBanktype(tTBssPayInfo.Bank_type);   //BankType
		pDepositInfo->setBankaccttype(tTBssPayInfo.Bankacct_type);  //BankAcctType
		pDepositInfo->setBankacctnum(tTBssPayInfo.BankacctNum);  //BankAcctNum
/*  有错		
		pDepositInfo->PAYMENTMETHODGROUP[0]->getMediumcode(tTBssPayInfo.CheckNum);  //check_num
		//还是下面这个？
		//pDepositInfo->setPaymentmethodgroup(value)
*/		
		if(-1 != tRuleMapInfo.lCYCLE_UPPER)
			pDepositInfo->setCycleupper(tRuleMapInfo.lCYCLE_UPPER);		

		if(-1 != tRuleMapInfo.lCYCLE_LOWER)
			pDepositInfo->setCyclelower(tRuleMapInfo.lCYCLE_LOWER );
			
		//Add by pengqiang5 for TJ 快补
		if(tTBssPayInfo.m_lDiscRate != -1L && tTBssPayInfo.m_lKBFlag == 2L)
		{
			pDepositInfo->setAutoconferflag(0);	//快补不能自动赠送
			pDepositInfo->setDiscrate(tTBssPayInfo.m_lDiscRate);
			pDepositInfo->setDiscratetype(tTBssPayInfo.m_lKBFlag);
			
			long lDepositAmount = tTBssPayInfo.m_lADDUP_SUMLIMIT - tTBssPayInfo.m_lTerminalAmount;
			if(lDepositAmount > 0L)	
			{
				//有本金预存，就走本金+赠送方式
				pDepositInfo->setAmount(lDepositAmount);
				pDepositInfo->setAcctbalanceamount(lDepositAmount);
			
				//终端款赠送部分
				T_CONFERVALUE* pConferValue = new T_CONFERVALUE;
				if(pConferValue == NULL)
				{
					THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
				}
			
				pConferValue->setConfertypeid(1);
				pConferValue->setConfervalue(tTBssPayInfo.m_lTerminalAmount);
				pConferValue->setConferbalancetypeid(tRuleMapInfo.lConferBalanceTypeID);
				pConferValue->setReturnruleid(tRuleMapInfo.lRETURN_RULE_ID);
				pConferValue->setConferruleid(-1);
				pConferValue->setJudgeconditionid(-1);
				pConferValue->setBalancetypename("-1");
				pConferValue->setConfertypedesc("-1");
				pConferValue->setReturnrulename("-1");
				pConferValue->setReturnobjecttype(1);
				pConferValue->setMonreturnflag(1);
				pConferValue->setCycleupper(-1);
				pConferValue->setCyclelower(-1);
				pDepositInfo->getConfervalue().push_back(pConferValue);
			}
			else
			{
				//没有本金，直接走赠送预存方式
				pDepositInfo->setBalancetypeid(tRuleMapInfo.lConferBalanceTypeID);
				pDepositInfo->setAmount(tTBssPayInfo.m_lTerminalAmount);
				pDepositInfo->setAcctbalanceamount(tTBssPayInfo.m_lTerminalAmount);
			}
		}
		else
		{
			pDepositInfo->setAmount(tRuleMapInfo.lCHARGE);
			pDepositInfo->setAcctbalanceamount(tRuleMapInfo.lCHARGE);
			pDepositInfo->setAutoconferflag(tRuleMapInfo.lCONFER_FLAG);
		}		
		
		///是否分期返还;
		if(-1 != tRuleMapInfo.lRETURN_RULE_ID ){
			pReturnRule = new T_RETURNRULE;
			if ( ! pReturnRule){
				THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
			}
		
			sRet = m_DepositReturn->getReturnRuleByRuleID(tRuleMapInfo.lRETURN_RULE_ID,pReturnRule);
			if (sRet.compare(SUCCESS)){
				THROW_BILL(SYS_ERR_CODE, "" ,sRet); 
			}
			
			vector <T_RETURNRULE*>& rvReturnRule = pDepositInfo->getReturnrule();
			rvReturnRule.clear();
			rvReturnRule.push_back(pReturnRule);
			pReturnRule = NULL;
		}
		//翼支付返还流程
		if (1L == tTBssPayInfo.m_lKBFlag)
		{
			iReturnFlag = 1;
			pDepositInfo->setIfreturn(iReturnFlag);

			sRet.clear();
			sRet = m_AcctBalanceMgr->DepositReturnForBestPay(pDepositInfo ,tTBssPayInfo.m_lPROD_2_PP_ID);
			if (sRet.compare(SUCCESS))
			{
				THROW_BILL(SYS_ERR_CODE, "" ,sRet);
			}
		}
		else
		{

			pCashBill = new T_CASHBILL;
			if ( ! pCashBill){
				THROW_BILL(SYS_ERR_CODE, "", MALLOC_ERROR_STR );
			}
			//填写付款信息
			vector <T_PAYMENTMETHODGROUP *>&rvPaymentMethodGroup = pCashBill->getPaymentmethodgroup();
			rvPaymentMethodGroup.clear();		
			
			//获取欠费信息
			vector<T_FEEBILL *> &rvFeeBill = pCashBill->getFeebill();
			rvFeeBill.clear();
			
			//获取可用余额明细
			vector< T_VALBALANCEDETAIL *>&rvValBalanceDetail = pCashBill->getValbalancedetail();
			rvValBalanceDetail.clear();
			
			pCashBill->setBillmethod(BILL_METHOD_DEPOSIT );
			pCashBill->setAccnbr(sAccNbr_SRC );
			
			vector<T_DEPOSITINFO*>& rvDepositInfo = pCashBill->getDepositinfo();
			rvDepositInfo.clear();
			rvDepositInfo.push_back(pDepositInfo);
			pDepositInfo = NULL;
    	
    	
			sRet.clear();
			sRet = m_AcctBalanceMgr->BillInterface(pCashBill);
			if (sRet.compare(SUCCESS))
			{
				THROW_BILL(SYS_ERR_CODE, "" ,sRet); 
			}
			
			tTBssPayInfo.lPaymentID = lPaymentID = (long)pCashBill->getPayserialno() ;
			tTBssPayInfo.lAmount =  (long)pCashBill->getAmount() ;

    	
			userlog("bss_funds_inter: Serv_ID[%ld],sAccNbr[%s],Amount[%ld],PaymentID[%ld]" 
				,lServID ,sAccNbr_SRC
,tRuleMapInfo.lCHARGE ,lPaymentID);
		}
	}
	catch(BillException &bex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = bex.getErrCode();
		sErrMsg = bex.getErrMsg();
		sRet = sContext = bex.getContext();
	}
	catch(exception &ex)
	{
		lRet = SYS_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = ex.what();
	}
	catch(...)
	{
		lRet = OTHER_ERR_CODE;
		lErrCode = lRet ;
		sRet = sErrMsg = "Other Error";
	}

	

	//###释放空间----;
	RELEASE_CLASS_INSTANCE( pServInfoQuery);
	RELEASE_CLASS_INSTANCE( pDepositInfo)  ;
	RELEASE_CLASS_INSTANCE( pReturnRule ) ;
	RELEASE_CLASS_INSTANCE( pCashBill );

	//////------component---
	RELEASE_CLASS_INSTANCE( pAgentBillMgr );
	RELEASE_CLASS_INSTANCE( pBssRuleMapPrs );
	RELEASE_CLASS_INSTANCE( m_AcctBalanceMgr );
	
	///////---vector----
	///DELETE_VECT_CLEAR(  );

	
	if( NO_ERROR != lRet )
	{
		pErrMsgProcess = new C_ERRMSG_PROCESS(gpACCT_DBLink);
		string sErrCodeMsg;
		pErrMsgProcess->getErrMsg(sContext, sErrCodeMsg);
		delete pErrMsgProcess;
		pErrMsgProcess = NULL;
		
		sprintf(sRltErrMsg,"错:%s.SQL代码[%ld].CodeMsg[%s].sContext[%s]"
			,sErrMsg.c_str(), lErrCode, sErrCodeMsg.c_str() , sContext.c_str() );
		//##记录出错信息;
		userlog(sRltErrMsg);		

		rsContext = sRltErrMsg  ;
	}	

	return sRet;
}
//add begin by xlfei on 20170420 for 增加未激活用户返赠款的处理
string C_bss_rule_map_prs::jugde_2hn_deposit_offer(const int& lOfferID, bool& bExist)
{
	string sRet = SUCCESS;
	string sSQL;
	int iPosition = 1;
	try
	{
		sSQL.clear();
		sSQL  = "SELECT offer_id ";
		sSQL += "  FROM deposit_2hn_rent a " ;
		sSQL += "where a.offer_id = :offer_id and a.state = '10A'" ;
		newQuery();
		setSQL(sSQL);
		setParameter(iPosition, lOfferID);
		executeQuery();
		if( m_rs->next() )
			{
					bExist = true;
			}				
		closeResultSet();
		close();
	}
	catch(SQLException &oraex)
	{
		userlog("[%s]_[%d] : oraex.ErrorCode(%d),  ErrMsg[%s]" , __FILE__ , __LINE__,oraex.getErrorCode(), oraex.getMessage().c_str() );		 
		closeResultSet();
		close();
		sRet = oraex.getMessage();
	}
	catch(BillException &bex)
	{
		sRet = bex.getContext();	
	}
	catch(exception &ex)
	{
		sRet = ex.what();		
	}
	catch(...)
	{
		sRet = UNKNOW_EXCEPTION;		
	}	
	return sRet;
}


