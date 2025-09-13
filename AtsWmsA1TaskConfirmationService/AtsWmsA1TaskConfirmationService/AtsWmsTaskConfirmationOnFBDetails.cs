using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AtsWmsA1TaskConfirmationService.ats_mahindra_databaseDataSetTableAdapters;
using static AtsWmsA1TaskConfirmationService.ats_mahindra_databaseDataSet;
using System.Net.NetworkInformation;
using OPCAutomation;
using System.Threading;
using log4net;
using System.Runtime.ExceptionServices;
using System.Security;

namespace AtsWmsA1TaskConfirmationService
{
    class AtsWmsTaskConfirmationOnFBDetails
    {


        #region DataTables
        ats_wms_master_plc_connection_detailsDataTable ats_wms_master_plc_connection_detailsDataTableDT = null;
        ats_wms_outfeed_mission_runtime_detailsDataTable ats_wms_outfeed_mission_runtime_detailsDataTableDT = null;
        ats_wms_infeed_mission_runtime_detailsDataTable ats_wms_infeed_mission_runtime_detailsDataTableDT = null;
        ats_wms_infeed_mission_runtime_detailsDataTable ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed = null;
        ats_wms_current_stock_detailsDataTable ats_wms_current_stock_detailsDataTableDT = null;
        ats_wms_master_pallet_informationDataTable ats_wms_master_pallet_informationDataTableDT = null;
        ats_wms_master_pallet_informationDataTable ats_wms_master_pallet_informationDataTableInfeedToOutfeed = null;
        ats_wms_master_pallet_informationDataTable ats_wms_master_pallet_informationDataTableCheckDummyPallet = null;
        ats_wms_current_stock_detailsDataTable ats_wms_current_stock_detailsDataTableCkeckDummypalletDT = null;


        ats_wms_current_stock_detailsDataTable ats_wms_current_stock_detailsDataTableDataRecheck = null;
        ats_wms_master_position_detailsDataTable ats_wms_master_position_detailsDataTableDTDataRecheck1 = null;
        ats_wms_master_position_detailsDataTable ats_wms_master_position_detailsDataTableDTDataRecheck2 = null;
        ats_wms_infeed_mission_runtime_detailsDataTable ats_wms_infeed_mission_runtime_detailsDataTableDataRechck = null;




        ats_wms_master_position_detailsDataTable ats_wms_master_position_detailsDataTableDT = null;

        #endregion

        #region TableAdapters
        ats_wms_master_plc_connection_detailsTableAdapter ats_wms_master_plc_connection_detailsTableAdapterInstance = new ats_wms_master_plc_connection_detailsTableAdapter();
        ats_wms_outfeed_mission_runtime_detailsTableAdapter ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance = new ats_wms_outfeed_mission_runtime_detailsTableAdapter();
        ats_wms_infeed_mission_runtime_detailsTableAdapter ats_wms_infeed_mission_runtime_detailsTableAdapterInstance = new ats_wms_infeed_mission_runtime_detailsTableAdapter();
        ats_wms_current_stock_detailsTableAdapter ats_wms_current_stock_detailsTableAdapterInstance = new ats_wms_current_stock_detailsTableAdapter();
        ats_wms_master_position_detailsTableAdapter ats_wms_master_position_detailsTableAdapterInstance = new ats_wms_master_position_detailsTableAdapter();
        ats_wms_master_pallet_informationTableAdapter ats_wms_master_pallet_informationTableAdapterInstance = new ats_wms_master_pallet_informationTableAdapter();


        #endregion

        #region PLC PING VARIABLE   
        //private string IP_ADDRESS = System.Configuration.ConfigurationManager.AppSettings["IP_ADDRESS"]; //2
        private Ping pingSenderForThisConnection = null;
        private PingReply replyForThisConnection = null;
        private Boolean pingStatus = false;
        private int serverPingStatusCount = 0;
        #endregion

        #region KEPWARE VARIABLES

        /* Kepware variable*/

        OPCServer ConnectedOpc = new OPCServer();

        Array OPCItemIDs = Array.CreateInstance(typeof(string), 100);
        Array ItemServerHandles = Array.CreateInstance(typeof(Int32), 100);
        Array ItemServerErrors = Array.CreateInstance(typeof(Int32), 100);
        Array ClientHandles = Array.CreateInstance(typeof(Int32), 100);
        Array RequestedDataTypes = Array.CreateInstance(typeof(Int16), 100);
        Array AccessPaths = Array.CreateInstance(typeof(string), 100);
        Array ItemServerValues = Array.CreateInstance(typeof(string), 100);
        OPCGroup OpcGroupNames;
        object aDIR;
        object bDIR;

        // Connection string
        static string plcServerConnectionString = null;

        #endregion

        #region Global Variables
        static string className = "AtsWmsTaskConfirmationOnFBDetails";
        private static readonly ILog Log = LogManager.GetLogger(className);
        private System.Timers.Timer AtsWmsTaskConfirmationOnFBDetailsTimer = null;
        private string IP_ADDRESS = "192.168.0.1";

        string currentDate = "";
        string currentTime = "";
        int areaId = 1;
        int positionNumberInRack = 0;
        public int stackerAreaSide = 0;
        public int stackerFloor = 0;
        public int stackerColumn = 0;
        public int destinationPositionNumberInRack = 1;
        int palletPresentOnStackerPickupPosition = 0;
        string palletCodeOnStackerPickupPosition = "";
        int stackerRightSide = 2;
        int stackerLeftSide = 1;
        int sourcePositionTagType = 0;
        int destinationPositionTagType = 1;
        int feedbackTagType = 2;

        int maxRetryAttempts = 3;
        int delayBetweenRetriesMs = 1000;


        string TASK_CONFIRMATION_TAG = "ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION";
        string DATA_REQUEST_TAG = "ATS.WMS_AREA_1.AREA_1_STACKER_1_DATA_REQUEST";
        string TASK_COMPLETION = "ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_COMPLETION";
        string TASK_NO = "ATS.WMS_AREA_1.STACKER_1_FB_TASK_NO";
        string TASk_TYPE = "ATS.WMS_AREA_1.STACKER_1_FB_TASK_TYPE";
        #endregion

        public void startOperation()
        {
            try
            {
                Log.Debug("1. startOperation");
                //Timer 
                AtsWmsTaskConfirmationOnFBDetailsTimer = new System.Timers.Timer();

                //Running the function after 1 sec 
                AtsWmsTaskConfirmationOnFBDetailsTimer.Interval = (1000);

                //to reset timer after completion of 1 cycle


                //Enabling the timer
                //AtsWmsTaskConfirmationOnFBDetailsTimer.Enabled = true;

                //Timer Start


                //After 1 sec timer will elapse and DataFetchDetailsOperation function will be called 
                AtsWmsTaskConfirmationOnFBDetailsTimer.Elapsed += new System.Timers.ElapsedEventHandler(AtsWmsTaskConfirmationOnFBDetailsOperation);
                AtsWmsTaskConfirmationOnFBDetailsTimer.AutoReset = false;
                AtsWmsTaskConfirmationOnFBDetailsTimer.Start();
            }
            catch (Exception ex)
            {
                Log.Error("startOperation :: Exception Occure in AtsWmsTaskConfirmationOnFBDetailsTimer" + ex.Message);
            }
        }


        public void AtsWmsTaskConfirmationOnFBDetailsOperation(object sender, EventArgs args)
        {
            try
            {
                try
                {
                    Log.Debug("2. In AtsWmsTaskConfirmationOnFBDetailsOperation");
                    //Stopping the timer to start the below operation
                    AtsWmsTaskConfirmationOnFBDetailsTimer.Stop();
                    Log.Debug("3. AtsWmsTaskConfirmationOnFBDetailsTimer timer stop");
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: Exception occure while stopping the timer :: " + ex.Message + "StackTrace  :: " + ex.StackTrace);
                }

                try
                {
                    //Fetching PLC data from DB by sending PLC connection IP address
                    ats_wms_master_plc_connection_detailsDataTableDT = ats_wms_master_plc_connection_detailsTableAdapterInstance.GetDataByPLC_CONNECTION_IP_ADDRESS(IP_ADDRESS);
                    Log.Debug("4. PlC connection");
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: Exception Occure while reading machine datasource connection details from the database :: " + ex.Message + "StackTrace :: " + ex.StackTrace);
                }


                // Check PLC Ping Status
                try
                {
                    //Checking the PLC ping status by a method
                    pingStatus = checkPlcPingRequest();
                    Log.Debug("5. ping" + pingStatus);
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: Exception while checking plc ping status :: " + ex.Message + " stactTrace :: " + ex.StackTrace);
                }

                if (pingStatus == true)

                //if (true)
                {
                    //checking if the PLC data from DB is retrived or not
                    if (ats_wms_master_plc_connection_detailsDataTableDT != null && ats_wms_master_plc_connection_detailsDataTableDT.Count != 0)
                    //if (true)
                    {
                        Log.Debug("6 .plc conmnect");
                        try
                        {
                            plcServerConnectionString = ats_wms_master_plc_connection_detailsDataTableDT[0].PLC_CONNECTION_URL;
                        }
                        catch (Exception ex)
                        {
                            Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: Exception while Checking plcServerConnectionString :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                        }
                        try
                        {
                            //Calling the connection method for PLC connection
                            OnConnectPLC();
                        }
                        catch (Exception ex)
                        {
                            Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                        }

                        // Check the PLC connected status
                        if (ConnectedOpc.ServerState.ToString().Equals("1"))
                        //if (true)
                        {

                            try
                            {
                                string taskCompletionValue = "";
                                try
                                {
                                    taskCompletionValue = readTag(TASK_COMPLETION);

                                }
                                catch (Exception ex)
                                {
                                    Log.Error("Exception occurred while reading pallet present :: " + ex.StackTrace);
                                }

                                if (taskCompletionValue.Equals("9"))
                                {
                                    Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: taskCompletionValue " + taskCompletionValue);
                                    Log.Debug("1");
                                    string fbTaskNo = "";
                                    try
                                    {

                                        fbTaskNo = readTag(TASK_NO);
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("Exception occurred while reading task number :: " + ex.StackTrace);
                                    }

                                    Log.Debug("2");
                                    string fbtaskType = "";
                                    try
                                    {

                                        fbtaskType = readTag(TASk_TYPE);
                                        Log.Debug("3");
                                        Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: fbTaskNo: " + fbTaskNo + " fbtaskType: " + fbtaskType);
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("Exception occurred while reading Task Type :: " + ex.StackTrace);
                                    }


                                    int fbTaskNoInt = 0;
                                    String timeNow = DateTime.Now.TimeOfDay.ToString();
                                    TimeSpan currentTimeNow = TimeSpan.Parse(timeNow);

                                    String currentDate = "";
                                    currentDate = Convert.ToString(DateTime.Now.ToString("yyyy-MM-dd"));
                                    try
                                    {
                                        fbTaskNoInt = Int32.Parse(fbTaskNo);
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: exception occurred while converting to int :: " + ex.Message);
                                    }
                                    // INFEED MISSION



                                    Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: INFEED MISSION");
                                    try
                                    {
                                        ats_wms_infeed_mission_runtime_detailsDataTableDT = ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.GetDataByINFEED_MISSION_IDAndINFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: exception occurred while infeed mission details :: " + ex.Message);
                                    }

                                    try
                                    {
                                        Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION");
                                        ats_wms_outfeed_mission_runtime_detailsDataTableDT = ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance.GetDataByOUTFEED_MISSION_IDAndOUTFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                    }
                                    catch (Exception ex)
                                    {

                                        Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: exception occurred while outfeed mission details :: " + ex.Message);
                                    }


                                    if ((ats_wms_infeed_mission_runtime_detailsDataTableDT != null && ats_wms_infeed_mission_runtime_detailsDataTableDT.Count > 0) || (ats_wms_outfeed_mission_runtime_detailsDataTableDT != null && ats_wms_outfeed_mission_runtime_detailsDataTableDT.Count > 0))
                                    {


                                        if (fbtaskType.Equals("1"))
                                        {


                                            Log.Debug("3");
                                            string fbTargetColumn = "";
                                            try
                                            {
                                                fbTargetColumn = readTag("ATS.WMS_AREA_1.STACKER_1_FB_TARGET_COLUMN");
                                                Log.Debug("fbTargetColumn :: " + fbTargetColumn);

                                                if (fbTargetColumn.Length == 1)
                                                {
                                                    fbTargetColumn = "0" + fbTargetColumn;
                                                }
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("Exception occurred while reading fbTargetColumn :: " + ex.StackTrace);
                                            }



                                            Log.Debug("4");
                                            string fbTargetFloor = "";
                                            try
                                            {
                                                fbTargetFloor = readTag("ATS.WMS_AREA_1.STACKER_1_FB_TARGET_FLOOR");
                                                Log.Debug("fbTargetFloor :: " + fbTargetFloor);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("Exception occurred while reading fbTargetFloor :: " + ex.StackTrace);
                                            }

                                            Log.Debug("5");
                                            string fbTargetLine = "";
                                            try
                                            {
                                                fbTargetLine = readTag("ATS.WMS_AREA_1.STACKER_1_FB_TARGET_LINE");
                                                Log.Debug("fbTargetLine :: " + fbTargetLine);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("Exception occurred while reading fbTargetLine :: " + ex.StackTrace);
                                            }


                                            //Converting to position name
                                            string feedbackPositionName = "";
                                            int positionID = 0;
                                            feedbackPositionName = "A01-0" + fbTargetLine + "-" + fbTargetColumn + "-0" + fbTargetFloor;

                                            Log.Debug("feedbackPositionName :: " + feedbackPositionName);
                                            ats_wms_master_position_detailsDataTableDT = ats_wms_master_position_detailsTableAdapterInstance.GetDataByPOSITION_NAME(feedbackPositionName);
                                            if (ats_wms_master_position_detailsDataTableDT != null && ats_wms_master_position_detailsDataTableDT.Count > 0)
                                            {
                                                Log.Debug("Found Postion ID :: " + ats_wms_master_position_detailsDataTableDT[0].POSITION_ID);
                                                positionID = ats_wms_master_position_detailsDataTableDT[0].POSITION_ID;
                                            }



                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: INFEED MISSION");
                                            try
                                            {
                                                ats_wms_infeed_mission_runtime_detailsDataTableDT = ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.GetDataByINFEED_MISSION_IDAndINFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: exception occurred while infeed mission details :: " + ex.Message);
                                            }

                                            try
                                            {
                                                ats_wms_master_pallet_informationDataTableDT = ats_wms_master_pallet_informationTableAdapterInstance.GetDataByPALLET_INFORMATION_ID(ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation :: exception occurred while pallet information details details :: " + ex.Message);
                                            }

                                            if (ats_wms_infeed_mission_runtime_detailsDataTableDT != null && ats_wms_infeed_mission_runtime_detailsDataTableDT.Count > 0 && ats_wms_master_pallet_informationDataTableDT != null && ats_wms_master_pallet_informationDataTableDT.Count > 0)
                                            {
                                                try
                                                {

                                                    {
                                                        Log.Debug("GiveStackerMission :: Updating Position name against pallet information ID in Curent Pallet Stock details table");
                                                        // update the pallet data in stock table agaist the pallet position id

                                                        bool palletUpdateSuccess = ExecuteWithRetry(() =>
                                                        {
                                                            ats_wms_current_stock_detailsTableAdapterInstance.UpdatePALLET_INFORMATION_DETAILSWherePOSITION_ID(
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_CODE,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIENT_CODE,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_CODE_E2,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_ID,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_NAME,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_ID,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_NAME,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_NAME_E2,
                                                            ats_wms_master_pallet_informationDataTableDT[0].PALLET_STATUS_ID,
                                                            ats_wms_master_pallet_informationDataTableDT[0].PALLET_TYPE,
                                                            ats_wms_master_pallet_informationDataTableDT[0].PALLET_STATUS_NAME,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_1_CODE,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_2_CODE,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_1_WEIGHT,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_2_WEIGHT,
                                                            0,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].QUANTITY,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].LOCATION,
                                                            0, 1, 0,
                                                            0, 1,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].BATCH_NUMBER,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].MODEL_NUMBER,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].MODEL_NUMBER_E2,
                                                            ats_wms_infeed_mission_runtime_detailsDataTableDT[0].SERIAL_NO_STARTING_CHAR,
                                                            (currentDate + " " + currentTimeNow),
                                                            positionID);


                                                            return true;
                                                        }, "UpdatePALLET_INFORMATION_DETAILSWherePOSITION_ID");


                                                        if (!palletUpdateSuccess) Log.Debug("Failed to update pallet information");


                                                        // Step 2: Cross-check the updated data


                                                        bool dataCheckSuccess = ExecuteWithRetry(() =>
                                                        {
                                                            ats_wms_current_stock_detailsDataTableDataRecheck = ats_wms_current_stock_detailsTableAdapterInstance.GetDataByPALLET_CODEAndPALLET_INFORMATION_ID(ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_CODE, ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);

                                                            return ats_wms_current_stock_detailsDataTableDataRecheck != null && ats_wms_current_stock_detailsDataTableDataRecheck.Count > 0;

                                                        }, "GetDataByPALLET_CODEAndPALLET_INFORMATION_ID");


                                                        if (!dataCheckSuccess) Log.Debug("Data cross-check failed after updating pallet information in current stock");


                                                        // Step 3: Update Master Position Details

                                                        if (ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID != positionID)
                                                        {


                                                            bool positionUpdateSuccess = ExecuteWithRetry(() =>
                                                        {
                                                            ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ACTIVEAndPOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYAndIS_MANUAL_DISPATCHWherePOSITION_ID(0, 0, 1, 0, ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                            ats_wms_master_position_detailsDataTableDTDataRecheck1 = ats_wms_master_position_detailsTableAdapterInstance.GetDataByPOSITION_IDAndPOSITION_IS_EMPTYAndPOSITION_IS_ALLOCATED(ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID,
                                                                  1, 0);

                                                            return ats_wms_master_position_detailsDataTableDTDataRecheck1 != null && ats_wms_master_position_detailsDataTableDTDataRecheck1.Count > 0;

                                                        }, "UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID1");
                                                        }


                                                        ////Locking the infeed mission position if not the same as PLC feedback position
                                                        //if (ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID != positionID)
                                                        //{

                                                        //    Log.Debug("Locking the infeed mission position if not the same as PLC feedback position");
                                                        //    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ACTIVEAndPOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYAndIS_MANUAL_DISPATCHWherePOSITION_ID(0, 0, 1, 0, ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);
                                                        //}


                                                        if (positionID != 0)
                                                        {
                                                            bool positionUpdateSuccess = ExecuteWithRetry(() =>
                                                            {
                                                                ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(1, 0, positionID);

                                                                ats_wms_master_position_detailsDataTableDTDataRecheck2 = ats_wms_master_position_detailsTableAdapterInstance.GetDataByPOSITION_IDAndPOSITION_IS_EMPTYAndPOSITION_IS_ALLOCATED(ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID,
                                                                      0, 1);

                                                                return ats_wms_master_position_detailsDataTableDTDataRecheck2 != null && ats_wms_master_position_detailsDataTableDTDataRecheck2.Count > 0;

                                                            }, "UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID2");

                                                            ////Updating pallet position details in DB
                                                            //Log.Debug("Updating values in Master Position Details for PLC position :: " + positionID);
                                                            //ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(1, 0, positionID); 
                                                        }

                                                        // Step 4 : Update mission status

                                                        bool missionStatusUpdateSuccess = ExecuteWithRetry(() =>
                                                        {
                                                            Log.Debug("Updating Mission status as :: COMPLETED");
                                                            ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_infeed_mission_runtime_detailsDataTableDT[0].INFEED_MISSION_ID);

                                                            ats_wms_infeed_mission_runtime_detailsDataTableDataRechck = ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.GetDataByINFEED_MISSION_STATUSAndINFEED_MISSION_ID("COMPLETED", ats_wms_infeed_mission_runtime_detailsDataTableDT[0].INFEED_MISSION_ID);
                                                            return true;

                                                        }, "UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID");

                                                        if (!missionStatusUpdateSuccess) Log.Debug("Failed to update mission status");

                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("GiveStackerMission :: Exception occured while updating pallet information in DB :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                                                }

                                                bool updateTaskConformation = ExecuteWithRetry(() =>
                                                {
                                                    try
                                                    {
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : writing Task confirmation 1");
                                                        writeTag(TASK_CONFIRMATION_TAG, "1");
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                        for (;;)
                                                        {
                                                        //Thread.Sleep(1000);
                                                        writeTag(TASK_CONFIRMATION_TAG, "1");
                                                            Thread.Sleep(1000);

                                                            if (readTag(DATA_REQUEST_TAG).Equals("True"))
                                                            {
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : data request True");
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : writing Task confirmation 0");
                                                                writeTag(TASK_CONFIRMATION_TAG, "0");
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : Task confirmation 0 Successfully written");
                                                                break;
                                                            }
                                                        }
                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        Log.Error("Exception occurred while writting task confirmation:: " + ex.Message);
                                                    }
                                                    return true;
                                                }, "TaskConformationUpdate");

                                            }

                                        }

                                        else if (fbtaskType.Equals("2"))
                                        {
                                            //OUTFEED MISSION
                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION");
                                            ats_wms_outfeed_mission_runtime_detailsDataTableDT = ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance.GetDataByOUTFEED_MISSION_IDAndOUTFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                            if (ats_wms_outfeed_mission_runtime_detailsDataTableDT != null && ats_wms_outfeed_mission_runtime_detailsDataTableDT.Count > 0)
                                            {
                                                try
                                                {
                                                    Log.Debug("Updating Mission status as :: COMPLETED");
                                                    //need to conform this line
                                                    ats_wms_current_stock_detailsTableAdapterInstance.UpdatePALLET_INFORMATION_DETAILSWherePOSITION_ID("NA", 0, "NA", "NA", 0, "NA", 0, "NA", "NA", 0, "NA", "NA", "NA", "NA", 0, 0, 0, 0, "NA", 1, 0, 1, 0, 0, "NA", "NA", "NA", "NA", DateTime.Now.ToString(), ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //Updating pallet position details in DB

                                                    ats_wms_current_stock_detailsDataTableDT = ats_wms_current_stock_detailsTableAdapterInstance.GetDataByPALLET_CODEAndPALLET_INFORMATION_ID(ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_CODE, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);

                                                    Log.Debug("Updating values in Master Position Details");
                                                    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYAndIS_MANUAL_DISPATCH(0, 1, 0, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);
                                                    Log.Debug("Updating in master Position for POSITION_ID :: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //updating mission status to Completed in outfeed mission details table against outfeed mission id
                                                    Log.Debug("Updating Mission status as :: COMPLETED");
                                                    ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance.UpdateOUTFEED_MISSION_STATUSAndOUTFEED_MISSION_END_DATETIMEWhereOUTFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].OUTFEED_MISSION_ID);

                                                    Log.Debug("Updating :: is infeed and outfeed Mission generated to 0 :: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                                    ats_wms_master_pallet_informationTableAdapterInstance.UpdateIS_INFEED_MISSION_GENERATEDAndIS_OUTFEED_MISSION_GENERATEDWherePALLET_INFORMATION_ID(0, 0, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                                    Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : POSITION_ID: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);


                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("GiveOutfeedMissionToStackerDetails :: giveMissionToStacker :: Exception occured while updating pallet information in DB :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                                                }
                                                try
                                                {
                                                    Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 1");
                                                    writeTag(TASK_CONFIRMATION_TAG, "1");
                                                    Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                    for (;;)
                                                    {
                                                        //Thread.Sleep(1000);
                                                        writeTag(TASK_CONFIRMATION_TAG, "1");
                                                        Thread.Sleep(1000);

                                                        if (readTag(DATA_REQUEST_TAG).Equals("True"))
                                                        {
                                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : data request True");
                                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 0");
                                                            writeTag(TASK_CONFIRMATION_TAG, "0");
                                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 0 Successfully written");
                                                            break;
                                                        }
                                                    }
                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("Exception occurred while writting task confirmation:: " + ex.Message);

                                                }
                                            }

                                        }
                                    }
                                    else
                                    {
                                        if (readTag(DATA_REQUEST_TAG).Equals("True"))
                                        {
                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: data request True");
                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation ::  writing Task confirmation 0");
                                            writeTag(TASK_CONFIRMATION_TAG, "0");
                                            Log.Debug("AtsWmsTaskConfirmationOnFBDetailsOperation :: Task confirmation 0 Successfully written");
                                          
                                        }

                                    }


                                }

                            }
                            catch (Exception ex)
                            {
                                Log.Error("AtsWmsTaskConfirmationOnFBDetailsOperation ::  " + ex.Message + " Stacktrace:: " + ex.StackTrace);
                            }
                        }
                        else
                        {
                            //Reconnect to plc
                            Log.Debug("plc not connected");
                        }
                    }
                    else
                    {
                        //Reconnect to plc, Check Ip address, url
                    }
                }

            }
            catch (Exception ex)
            {

                Log.Error("startOperation :: Exception occured while stopping timer :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
            }
            finally
            {
                try
                {
                    //Starting the timer again for the next iteration
                    AtsWmsTaskConfirmationOnFBDetailsTimer.Start();
                }
                catch (Exception ex1)
                {
                    Log.Error("startOperation :: Exception occured while stopping timer :: " + ex1.Message + " stackTrace :: " + ex1.StackTrace);
                }

            }

        }



        bool ExecuteWithRetry(Func<bool> operation, string operationName)
        {
            int attempt = 0;
            while (attempt < maxRetryAttempts)
            {
                attempt++;
                try
                {
                    if (operation())
                    {
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"Attempt {attempt} failed for {operationName}: {ex.Message}");
                }
                // Delay before retrying
                if (attempt < maxRetryAttempts)
                {
                    Thread.Sleep(delayBetweenRetriesMs);
                }
            }
            Log.Error($"{operationName} failed after {maxRetryAttempts} attempts.");
            return false;
        }


        #region Ping funcationality

        public Boolean checkPlcPingRequest()
        {
            //Log.Debug("IprodPLCMachineXmlGenOperation :: Inside checkServerPingRequest");

            try
            {
                try
                {
                    pingSenderForThisConnection = new Ping();
                    replyForThisConnection = pingSenderForThisConnection.Send(IP_ADDRESS);
                }
                catch (Exception ex)
                {
                    Log.Error("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Exception occured while sending ping request :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                    replyForThisConnection = null;
                }

                if (replyForThisConnection != null && replyForThisConnection.Status == IPStatus.Success)
                {
                    //Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping success :: " + replyForThisConnection.Status.ToString());
                    return true;
                }
                else
                {
                    //Log.Debug("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Ping failed. ");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log.Error("checkPlcPingRequest :: for IP :: " + IP_ADDRESS + " Exception while checking ping request :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                return false;
            }
        }

        #endregion

        #region Read and Write PLC tag

        [HandleProcessCorruptedStateExceptions]
        public string readTag(string tagName)
        {

            try
            {
                //Log.Debug("IprodPLCCommunicationOperation :: Inside readTag.");

                // Set PLC tag
                OPCItemIDs.SetValue(tagName, 1);
                //Log.Debug("readTag :: Plc tag is configured for plc group.");

                // remove all group
                ConnectedOpc.OPCGroups.RemoveAll();
                //Log.Debug("readTag :: Remove all group.");

                // Kepware configuration                
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsTaskConfirmationOnFBDetailsGroupA1");
                OpcGroupNames.DeadBand = 0;
                OpcGroupNames.UpdateRate = 100;
                OpcGroupNames.IsSubscribed = true;
                OpcGroupNames.IsActive = true;
                OpcGroupNames.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                //Log.Debug("readTag :: Kepware properties configuration is complete.");

                // Read tag
                OpcGroupNames.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out aDIR, out bDIR);

                //Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));

                if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("True"))
                {
                    Log.Debug("readTag :: Found and Return True");
                    Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));
                    // ConnectedOpc.OPCGroups.Remove("AtsWmsA1GiveOutfeedMissionDetailsGroup");
                    return "True";
                }
                else if (Convert.ToString(ItemServerValues.GetValue(1)).Equals("False"))
                {
                    Log.Debug("readTag :: Found and Return False");
                    Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));
                    // ConnectedOpc.OPCGroups.Remove("AtsWmsA1GiveOutfeedMissionDetailsGroup");
                    return "False";
                }
                else
                {
                    Log.Debug("readTag ::  tag name :: " + tagName + " tag value :: " + Convert.ToString(ItemServerValues.GetValue(1)));
                    //ConnectedOpc.OPCGroups.Remove("AtsWmsA1GiveOutfeedMissionDetailsGroup");
                    return Convert.ToString(ItemServerValues.GetValue(1));
                }

            }
            catch (Exception ex)
            {
                Log.Error("readTag :: Exception while reading plc tag :: " + tagName + " :: " + ex.Message);
                OnConnectPLC();
            }

            Log.Debug("readTag :: Return False.. retun null.");

            return "False";
        }

        [HandleProcessCorruptedStateExceptions]
        public void writeTag(string tagName, string tagValue)
        {

            try
            {
                Log.Debug("IprodGiveMissionToStacker :: Inside writeTag.");

                // Set PLC tag
                OPCItemIDs.SetValue(tagName, 1);
                //Log.Debug("writeTag :: Plc tag is configured for plc group.");

                // remove all group
                ConnectedOpc.OPCGroups.RemoveAll();
                //Log.Debug("writeTag :: Remove all group.");

                // Kepware configuration                  
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsTaskConfirmationOnFBDetailsGroupA1");
                OpcGroupNames.DeadBand = 0;
                OpcGroupNames.UpdateRate = 100;
                OpcGroupNames.IsSubscribed = true;
                OpcGroupNames.IsActive = true;
                OpcGroupNames.OPCItems.AddItems(1, ref OPCItemIDs, ref ClientHandles, out ItemServerHandles, out ItemServerErrors, RequestedDataTypes, AccessPaths);
                //Log.Debug("writeTag :: Kepware properties configuration is complete.");

                // read plc tags
                OpcGroupNames.SyncRead((short)OPCAutomation.OPCDataSource.OPCDevice, 1, ref
                   ItemServerHandles, out ItemServerValues, out ItemServerErrors, out aDIR, out bDIR);

                // Add tag value
                ItemServerValues.SetValue(tagValue, 1);

                // Write tag
                OpcGroupNames.SyncWrite(1, ref ItemServerHandles, ref ItemServerValues, out ItemServerErrors);
                //  ConnectedOpc.OPCGroups.Remove("AtsWmsA1GiveOutfeedMissionDetailsGroup");
                //return true;

            }
            catch (Exception ex)
            {
                Log.Error("writeTag :: Exception while writing mission data in the plc tag :: " + tagName + " :: " + ex.Message + " stackTrace :: " + ex.StackTrace);

                OnConnectPLC();
                Thread.Sleep(1000);

                Log.Debug("writing again :: tagName" + tagName + " tagValue :: " + tagValue);
                writeTag(tagName, tagValue);
            }

            //return false;

        }

        #endregion

        #region Connect and Disconnect PLC

        private void OnConnectPLC()
        {

            Log.Debug("OnConnectPLC :: inside OnConnectPLC");

            try
            {
                // Connection url
                if (!((ConnectedOpc.ServerState.ToString()).Equals("1")))
                {
                    ConnectedOpc.Connect(plcServerConnectionString, "");
                    Log.Debug("OnConnectPLC :: PLC connection successful and OPC server state is :: " + ConnectedOpc.ServerState.ToString());
                }
                else
                {
                    Log.Debug("OnConnectPLC :: Already connected with the plc.");
                }

            }
            catch (Exception ex)
            {
                Log.Error("OnConnectPLC :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
            }
        }

        private void OnDisconnectPLC()
        {
            Log.Debug("inside OnDisconnectPLC");

            try
            {
                ConnectedOpc.Disconnect();
                Log.Debug("OnDisconnectPLC :: Connection with the plc is disconnected.");
            }
            catch (Exception ex)
            {
                Log.Error("OnDisconnectPLC :: Exception while disconnecting to plc :: " + ex.Message);
            }

        }


        #endregion

    }
}


