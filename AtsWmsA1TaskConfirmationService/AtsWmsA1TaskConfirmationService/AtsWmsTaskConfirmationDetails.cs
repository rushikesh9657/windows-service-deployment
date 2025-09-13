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
    class AtsWmsTaskConfirmationDetails
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
        static string className = "AtsWmsTaskConfirmationDetails";
        private static readonly ILog Log = LogManager.GetLogger(className);
        private System.Timers.Timer AtsWmsTaskConfirmationDetailsTimer = null;
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
        #endregion

        public void startOperation()
        {
            try
            {
                Log.Debug("1. startOperation");
                //Timer 
                AtsWmsTaskConfirmationDetailsTimer = new System.Timers.Timer();

                //Running the function after 1 sec 
                AtsWmsTaskConfirmationDetailsTimer.Interval = (1000);

                //to reset timer after completion of 1 cycle


                //Enabling the timer
                //AtsWmsTaskConfirmationDetailsTimer.Enabled = true;

                //Timer Start


                //After 1 sec timer will elapse and DataFetchDetailsOperation function will be called 
                AtsWmsTaskConfirmationDetailsTimer.Elapsed += new System.Timers.ElapsedEventHandler(AtsWmsTaskConfirmationDetailsOperation);
                AtsWmsTaskConfirmationDetailsTimer.AutoReset = false;
                AtsWmsTaskConfirmationDetailsTimer.Start();
            }
            catch (Exception ex)
            {
                Log.Error("startOperation :: Exception Occure in AtsWmsTaskConfirmationDetailsTimer" + ex.Message);
            }
        }


        public void AtsWmsTaskConfirmationDetailsOperation(object sender, EventArgs args)
        {
            try
            {
                try
                {
                    Log.Debug("2. In AtsWmsTaskConfirmationDetailsOperation");
                    //Stopping the timer to start the below operation
                    AtsWmsTaskConfirmationDetailsTimer.Stop();
                    Log.Debug("3. AtsWmsTaskConfirmationDetailsTimer timer stop");
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsTaskConfirmationDetailsOperation :: Exception occure while stopping the timer :: " + ex.Message + "StackTrace  :: " + ex.StackTrace);
                }

                try
                {
                    //Fetching PLC data from DB by sending PLC connection IP address
                    ats_wms_master_plc_connection_detailsDataTableDT = ats_wms_master_plc_connection_detailsTableAdapterInstance.GetDataByPLC_CONNECTION_IP_ADDRESS(IP_ADDRESS);
                    Log.Debug("4. PlC connection");
                }
                catch (Exception ex)
                {
                    Log.Error("AtsWmsTaskConfirmationDetailsOperation :: Exception Occure while reading machine datasource connection details from the database :: " + ex.Message + "StackTrace :: " + ex.StackTrace);
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
                    Log.Error("AtsWmsTaskConfirmationDetailsOperation :: Exception while checking plc ping status :: " + ex.Message + " stactTrace :: " + ex.StackTrace);
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
                            Log.Error("AtsWmsTaskConfirmationDetailsOperation :: Exception while Checking plcServerConnectionString :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                        }
                        try
                        {
                            //Calling the connection method for PLC connection
                            OnConnectPLC();
                        }
                        catch (Exception ex)
                        {
                            Log.Error("AtsWmsTaskConfirmationDetailsOperation :: Exception while connecting to plc :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
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
                                    taskCompletionValue = readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_COMPLETION");

                                }
                                catch (Exception ex)
                                {
                                    Log.Error("Exception occurred while reading pallet present :: " + ex.StackTrace);
                                }
                                if (taskCompletionValue.Equals("9"))
                                {
                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: taskCompletionValue " + taskCompletionValue);

                                    Log.Debug("1");
                                    string fbTaskNo = "";
                                    try
                                    {

                                        fbTaskNo = readTag("ATS.WMS_AREA_1.STACKER_1_FB_TASK_NO");
                                    }
                                    catch (Exception ex)
                                    {
                                        Log.Error("Exception occurred while reading task number :: " + ex.StackTrace);
                                    }

                                    Log.Debug("2");
                                    string fbtaskType = "";
                                    try
                                    {

                                        fbtaskType = readTag("ATS.WMS_AREA_1.STACKER_1_FB_TASK_TYPE");
                                        Log.Debug("3");
                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: fbTaskNo: " + fbTaskNo + " fbtaskType: " + fbtaskType);
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
                                        Log.Error("AtsWmsTaskConfirmationDetailsOperation :: exception occurred while converting to int :: " + ex.Message);
                                    }
                                    // INFEED MISSION
                                    if (fbtaskType.Equals("1"))
                                    {
                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION");
                                        try
                                        {
                                            ats_wms_infeed_mission_runtime_detailsDataTableDT = ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.GetDataByINFEED_MISSION_IDAndINFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                        }
                                        catch (Exception ex)
                                        {
                                            Log.Error("AtsWmsTaskConfirmationDetailsOperation :: exception occurred while infeed mission details :: " + ex.Message);
                                        }

                                        try
                                        {
                                            ats_wms_master_pallet_informationDataTableDT = ats_wms_master_pallet_informationTableAdapterInstance.GetDataByPALLET_INFORMATION_ID(ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                        }
                                        catch (Exception ex)
                                        {
                                            Log.Error("AtsWmsTaskConfirmationDetailsOperation :: exception occurred while pallet information details details :: " + ex.Message);
                                        }

                                        if (ats_wms_infeed_mission_runtime_detailsDataTableDT != null && ats_wms_infeed_mission_runtime_detailsDataTableDT.Count > 0 && ats_wms_master_pallet_informationDataTableDT != null && ats_wms_master_pallet_informationDataTableDT.Count > 0)
                                        {
                                            try
                                            {
                                                //ats_wms_current_stock_detailsTableAdapterInstance.Insert(
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_CODE,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIENT_CODE,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_CODE_E2,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_ID,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_NAME,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_ID,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_NAME,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].PRODUCT_VARIANT_NAME_E2,
                                                //    ats_wms_master_pallet_informationDataTableDT[0].PALLET_STATUS_ID,
                                                //    ats_wms_master_pallet_informationDataTableDT[0].PALLET_TYPE,
                                                //    ats_wms_master_pallet_informationDataTableDT[0].PALLET_STATUS_NAME,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_1_CODE,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_2_CODE,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_1_WEIGHT,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].ENGINE_2_WEIGHT,
                                                //    0,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].QUANTITY,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].BATCH_NUMBER,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].MODEL_NUMBER,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].MODEL_NUMBER_E2,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].LOCATION,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_NAME,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_NUMBER_IN_RACK,
                                                //    1,
                                                //    0,1,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].RACK_ID,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].RACK_NAME,
                                                //    ats_wms_infeed_mission_runtime_detailsDataTableDT[0].RACK_SIDE,

                                                //    );


                                                // date 10-05-2024
                                                ////NOT UPDATING PALLET DATA IN CURRENT STOCK WHILE STACKER DOING OPERATION IN SEMI-AUTO


                                                //Removed Semi-auto check
                                                // Date 17-10-2024 

                                                //string stackerControlMode = "";
                                                //stackerControlMode = readTag("ATS.WMS_AREA_1.STACKER_1_CONTROL_MODE");
                                                //if (stackerControlMode.Equals("3"))
                                                {
                                                    Log.Debug("GiveStackerMission :: Updating Position name against pallet information ID in Curent Pallet Stock details table");
                                                    // update the pallet data in stock table agaist the pallet position id
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
                                                        ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //Updating pallet position details in DB
                                                    Log.Debug("Updating values in Master Position Details");
                                                    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(1, 0, ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //updating mission status to Completed in infeed mission details table against infeed mission id
                                                    Log.Debug("Updating Mission status as :: COMPLETED");
                                                    ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_infeed_mission_runtime_detailsDataTableDT[0].INFEED_MISSION_ID);
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);
                                                }
                                                //else
                                                //{
                                                //    //Updating pallet position details in DB
                                                //    Log.Debug("Updating values in Master Position Details in SEMI AUTO");
                                                //    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(1, 0, ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                //    //updating mission status to Completed in infeed mission details table against infeed mission id
                                                //    Log.Debug("Updating Mission status as :: COMPLETED");
                                                //    ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_infeed_mission_runtime_detailsDataTableDT[0].INFEED_MISSION_ID);
                                                //    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                //}
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("GiveStackerMission :: Exception occured while updating pallet information in DB :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                                            }
                                            try
                                            {
                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : writing Task confirmation 1");
                                                writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                for (;;)
                                                {
                                                    //Thread.Sleep(1000);
                                                    writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                    Thread.Sleep(1000);

                                                    if (readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_DATA_REQUEST").Equals("True"))
                                                    {
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : data request True");
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : writing Task confirmation 0");
                                                        writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "0");
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : Task confirmation 0 Successfully written");
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

                                    else if (fbtaskType.Equals("2"))
                                    { //OUTFEED MISSION

                                        try
                                        {
                                            ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed = ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.GetDataByINFEED_MISSION_IDAndINFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                        }
                                        catch (Exception ex)
                                        {
                                            Log.Error("AtsWmsTaskConfirmationDetailsOperation :: exception occurred while infeed mission details :: " + ex.Message);
                                        }

                                        if (ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed != null && ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed.Count > 0)
                                        {



                                            try
                                            {
                                                Log.Debug("Size Of List :: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed.Count);
                                                Log.Debug("pallet Code :: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_CODE);
                                                Log.Debug("Pallet Information Id :: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);

                                                ats_wms_master_pallet_informationDataTableInfeedToOutfeed = ats_wms_master_pallet_informationTableAdapterInstance.GetDataByPALLET_INFORMATION_ID(ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);
                                            }
                                            catch (Exception ex)
                                            {
                                                Log.Error("AtsWmsTaskConfirmationDetailsOperation :: exception occurred while pallet information details details :: " + ex.Message);
                                            }

                                            if (ats_wms_master_pallet_informationDataTableInfeedToOutfeed != null && ats_wms_master_pallet_informationDataTableInfeedToOutfeed.Count > 0)

                                            {

                                                //Updating pallet position details in DB
                                                Log.Debug("Updating values in Master Position Details in SEMI AUTO Infeed To Outfeed");
                                                ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(1, 0, ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);

                                                //updating mission status to Completed in infeed mission details table against infeed mission id
                                                Log.Debug("Updating Mission status as :: COMPLETED when Infeed To Outfeed");
                                                ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].INFEED_MISSION_ID);
                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);


                                                string palletAlreadypresent = "";
                                                try
                                                {
                                                    palletAlreadypresent = readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_PALLET_ALREADY_PRESENT");

                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("Exception occurred while reading pallet present :: " + ex.StackTrace);
                                                }

                                                if (palletAlreadypresent.Equals("True"))
                                                {

                                                    string palletAlreadypresentLH = "";
                                                    try
                                                    {
                                                        palletAlreadypresentLH = readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_PALLET_ALREADY_PRESENT_LH");

                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        Log.Error("Exception occurred while reading pallet present :: " + ex.StackTrace);
                                                    }

                                                    string palletAlreadypresentRH = "";
                                                    try
                                                    {
                                                        palletAlreadypresentRH = readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_PALLET_ALREADY_PRESENT_RH");

                                                    }
                                                    catch (Exception ex)
                                                    {
                                                        Log.Error("Exception occurred while reading pallet present :: " + ex.StackTrace);
                                                    }

                                                    if (palletAlreadypresentLH.Equals("True") || palletAlreadypresentRH.Equals("True"))
                                                    {



                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: Pallet Already Present True");



                                                        List<string> dummyPalletList = new List<string> { "9989", "9988", "9987", "9986", "9985", "9984", "9983", "9982", "9981", "9980" };

                                                        for (int i = 0; i < dummyPalletList.Count; i++)
                                                        {
                                                            Log.Debug("In dummyPalletList :: 1 :: dummyPalletList");

                                                            ats_wms_current_stock_detailsDataTableCkeckDummypalletDT = ats_wms_current_stock_detailsTableAdapterInstance.GetDataByPALLET_CODE(dummyPalletList[i]);
                                                            Log.Debug("In dummyPalletList :: 2 :: dummyPalletList :: ");

                                                            if (ats_wms_current_stock_detailsDataTableCkeckDummypalletDT != null && ats_wms_current_stock_detailsDataTableCkeckDummypalletDT.Count > 0)
                                                            {
                                                                continue;
                                                            }
                                                            else
                                                            {
                                                                Log.Debug("In dummyPalletList :: 3 :: dummyPalletList :: " + dummyPalletList[i]);

                                                                ats_wms_master_pallet_informationDataTableCheckDummyPallet = ats_wms_master_pallet_informationTableAdapterInstance.GetDataByPALLET_CODEAndPALLET_INFORMATION_IDOrderByDesc(dummyPalletList[i]);

                                                                if (ats_wms_master_pallet_informationDataTableCheckDummyPallet != null && ats_wms_master_pallet_informationDataTableCheckDummyPallet.Count > 0)
                                                                {
                                                                    Log.Debug("In dummyPalletList :: 4 :: dummyPalletList");
                                                                    try
                                                                    {
                                                                        Log.Debug("In dummyPalletList :: 5 :: dummyPalletList :: " + ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].PALLET_INFORMATION_ID);

                                                                        // update the pallet data in stock table agaist the pallet position id
                                                                        ats_wms_current_stock_detailsTableAdapterInstance.UpdatePALLET_INFORMATION_DETAILSWherePOSITION_ID(

                                                                            dummyPalletList[i],
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].PALLET_INFORMATION_ID,
                                                                            "NA",
                                                                             "NA",
                                                                            0,
                                                                             "NA",
                                                                            0,
                                                                             "NA",
                                                                             "NA",
                                                                            3,
                                                                             "NA",
                                                                            "EMPTY",
                                                                             "NA",
                                                                             "NA",
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].ENGINE_1_WEIGHT,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].ENGINE_2_WEIGHT,
                                                                            0,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].QUANTITY,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].LOCATION,
                                                                            0, 1, 0,
                                                                            0, 1,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].BATCH_NUMBER,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].MODEL_NUMBER,
                                                                            ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].MODEL_NUMBER_E2,
                                                                            "NA",
                                                                            (currentDate + " " + currentTimeNow),
                                                                            ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);

                                                                        Log.Debug("In dummyPalletList :: 6 :: Data Updated in Current Stock  :: " + dummyPalletList[i]);

                                                                        ats_wms_master_pallet_informationTableAdapterInstance.UpdatePALLET_STATUS_IDAndPALLET_STATUS_NAMEWherePALLET_INFORMATION_ID(3, "EMPTY", ats_wms_master_pallet_informationDataTableCheckDummyPallet[0].PALLET_INFORMATION_ID);

                                                                        Log.Debug("In dummyPalletList :: 6.1 :: Data Updated pallet info table  :: " + dummyPalletList[i]);
                                                                    }
                                                                    catch (Exception ex)
                                                                    {

                                                                        Log.Error("GiveStackerMission :: Exception occured while Updating Data :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                                                                    }


                                                                    Log.Debug("Updating :: is infeed and outfeed Mission generated to 0 :: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);
                                                                    ats_wms_master_pallet_informationTableAdapterInstance.UpdateIS_INFEED_MISSION_GENERATEDAndIS_OUTFEED_MISSION_GENERATEDWherePALLET_INFORMATION_ID(0, 0, ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);
                                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);


                                                                    try
                                                                    {
                                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 1");
                                                                        writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                                        for (;;)
                                                                        {
                                                                            // Thread.Sleep(1000);
                                                                            writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                                            Thread.Sleep(1000);

                                                                            if (readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_DATA_REQUEST").Equals("True"))
                                                                            {
                                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : data request True");
                                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 0");
                                                                                writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "0");
                                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 0 Successfully written");
                                                                                break;
                                                                            }
                                                                        }
                                                                    }
                                                                    catch (Exception ex)
                                                                    {
                                                                        Log.Error("Exception occurred while writting task confirmation:: " + ex.Message);

                                                                    }

                                                                }
                                                                else
                                                                {
                                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation:: No Dummy Pallet Found In PalletInfo table");
                                                                }
                                                            }


                                                        }
                                                    }



                                                }
                                                else
                                                {
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: Pallet Already Present False");

                                                    //Updating pallet position details in DB
                                                    Log.Debug("Updating values in Master Position Details in SEMI AUTO Infeed To Outfeed");
                                                    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYWherePOSITION_ID(0, 1, ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);

                                                    //updating mission status to Completed in infeed mission details table against infeed mission id
                                                    Log.Debug("Updating Mission status as :: COMPLETED when Infeed To Outfeed");
                                                    ats_wms_infeed_mission_runtime_detailsTableAdapterInstance.UpdateINFEED_MISSION_STATUSAndINFEED_MISSION_END_DATETIMEWhereINFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].INFEED_MISSION_ID);
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: INFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);


                                                    Log.Debug("Updating :: is infeed and outfeed Mission generated to 0 :: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);
                                                    ats_wms_master_pallet_informationTableAdapterInstance.UpdateIS_INFEED_MISSION_GENERATEDAndIS_OUTFEED_MISSION_GENERATEDWherePALLET_INFORMATION_ID(0, 0, ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].PALLET_INFORMATION_ID);
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : POSITION_ID: " + ats_wms_infeed_mission_runtime_detailsDataTableInfeedToOutfeed[0].POSITION_ID);


                                                    try
                                                    {
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 1");
                                                        writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                        Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                        for (;;)
                                                        {
                                                            // Thread.Sleep(1000);
                                                            writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                            Thread.Sleep(1000);

                                                            if (readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_DATA_REQUEST").Equals("True"))
                                                            {
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : data request True");
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 0");
                                                                writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "0");
                                                                Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 0 Successfully written");
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

                                            Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION");
                                            ats_wms_outfeed_mission_runtime_detailsDataTableDT = ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance.GetDataByOUTFEED_MISSION_IDAndOUTFEED_MISSION_STATUS(fbTaskNoInt, "IN_PROGRESS");
                                            if (ats_wms_outfeed_mission_runtime_detailsDataTableDT != null && ats_wms_outfeed_mission_runtime_detailsDataTableDT.Count > 0)
                                            {
                                                try
                                                {
                                                    Log.Debug("Updating Mission status as :: COMPLETED");

                                                    //Upadte the pallet data in stock table against the pallet position id 
                                                    Log.Debug("GiveStackerMission :: Updating Position name against pallet information ID in Curent Pallet Stock details table");
                                                    //ats_wms_current_stock_detailsTableAdapterInstance.UpdatePALLET_CODEAndPALLET_INFORMATION_IDAndORDER_IDAndFILLED_PERCENTAGEAndQUANTITYAndPOSITION_IS_EMPTYAndPOSITION_IS_ALLOCATED_FOR_MISSIONAndLOAD_DATETIMEAndIS_OUTFEED_MISSION_GENERATEDAndIS_INFEED_MISSION_GENERATEDWherePOSITION_ID(
                                                    //     "NA", 1, 0, 0, 0, 1, 0, (currentDate + " " + currentTimeNow), 1, 0, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //need to conform this line
                                                    ats_wms_current_stock_detailsTableAdapterInstance.UpdatePALLET_INFORMATION_DETAILSWherePOSITION_ID("NA", 0, "NA", "NA", 0, "NA", 0, "NA", "NA", 0, "NA", "NA", "NA", "NA", 0, 0, 0, 0, "NA", 1, 0, 1, 0, 0, "NA", "NA", "NA", "NA", DateTime.Now.ToString(), ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //Updating pallet position details in DB
                                                    Log.Debug("Updating values in Master Position Details");
                                                    ats_wms_master_position_detailsTableAdapterInstance.UpdatePOSITION_IS_ALLOCATEDAndPOSITION_IS_EMPTYAndIS_MANUAL_DISPATCH(0, 1, 0, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);
                                                    Log.Debug("Updating in master Position for POSITION_ID :: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);

                                                    //updating mission status to Completed in outfeed mission details table against outfeed mission id
                                                    Log.Debug("Updating Mission status as :: COMPLETED");
                                                    ats_wms_outfeed_mission_runtime_detailsTableAdapterInstance.UpdateOUTFEED_MISSION_STATUSAndOUTFEED_MISSION_END_DATETIMEWhereOUTFEED_MISSION_ID("COMPLETED", (currentDate + " " + currentTimeNow), ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].OUTFEED_MISSION_ID);

                                                    Log.Debug("Updating :: is infeed and outfeed Mission generated to 0 :: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                                    ats_wms_master_pallet_informationTableAdapterInstance.UpdateIS_INFEED_MISSION_GENERATEDAndIS_OUTFEED_MISSION_GENERATEDWherePALLET_INFORMATION_ID(0, 0, ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].PALLET_INFORMATION_ID);
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : POSITION_ID: " + ats_wms_outfeed_mission_runtime_detailsDataTableDT[0].POSITION_ID);
                                                }
                                                catch (Exception ex)
                                                {
                                                    Log.Error("GiveOutfeedMissionToStackerDetails :: giveMissionToStacker :: Exception occured while updating pallet information in DB :: " + ex.Message + " stackTrace :: " + ex.StackTrace);
                                                }
                                                try
                                                {
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 1");
                                                    writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                    Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 1 Successfully written");
                                                    for (;;)
                                                    {
                                                        //Thread.Sleep(1000);
                                                        writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "1");
                                                        Thread.Sleep(1000);

                                                        if (readTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_DATA_REQUEST").Equals("True"))
                                                        {
                                                            Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : data request True");
                                                            Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : writing Task confirmation 0");
                                                            writeTag("ATS.WMS_AREA_1.AREA_1_STACKER_1_TASK_CONFERMATION", "0");
                                                            Log.Debug("AtsWmsTaskConfirmationDetailsOperation :: OUTFEED MISSION DETAILS : Task confirmation 0 Successfully written");
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


                                }



                            }
                            catch (Exception ex)
                            {
                                Log.Error("AtsWmsTaskConfirmationDetailsOperation ::  " + ex.Message + " Stacktrace:: " + ex.StackTrace);
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
                    AtsWmsTaskConfirmationDetailsTimer.Start();
                }
                catch (Exception ex1)
                {
                    Log.Error("startOperation :: Exception occured while stopping timer :: " + ex1.Message + " stackTrace :: " + ex1.StackTrace);
                }

            }

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
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsTaskConfirmationDetailsGroupA1");
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
                OpcGroupNames = ConnectedOpc.OPCGroups.Add("AtsWmsTaskConfirmationDetailsGroupA1");
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


