using log4net;
using log4net.Config;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace AtsWmsA1TaskConfirmationService
{
    public partial class AtsWmsA1TaskConfirmationService : ServiceBase
    {

        static string className = "AtsWmsA1TaskConfirmationService";
        private static readonly ILog Log = LogManager.GetLogger(className);
        public AtsWmsA1TaskConfirmationService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {

            try
            {
                Log.Debug("OnStart :: AtsWmsTaskConfirmationService in OnStart....");

                try
                {
                    XmlConfigurator.Configure();
                    try
                    {
                        AtsWmsTaskConfirmationServiceTaskThread();
                    }
                    catch (Exception ex)
                    {
                        Log.Error("OnStart :: Exception occured while AtsWmsTaskConfirmationServiceTaskThread  threads task :: " + ex.Message);
                    }
                    Log.Debug("OnStart :: AtsWmsTaskConfirmationServiceTaskThread in OnStart ends..!!");
                }
                catch (Exception ex)
                {
                    Log.Error("OnStart :: Exception occured in OnStart :: " + ex.Message);
                }
            }
            catch (Exception ex)
            {
                Log.Error("OnStart :: Exception occured in OnStart :: " + ex.Message);
            }
        }

        public void AtsWmsTaskConfirmationServiceTaskThread()
        {

            try
            {
                //AtsWmsTaskConfirmationDetails AtsWmsTaskConfirmationDetailsInstance = new AtsWmsTaskConfirmationDetails();
                //AtsWmsTaskConfirmationDetailsInstance.startOperation();

                AtsWmsTaskConfirmationOnFBDetails AtsWmsTaskConfirmationOnFBDetailsInstance = new AtsWmsTaskConfirmationOnFBDetails();
                AtsWmsTaskConfirmationOnFBDetailsInstance.startOperation();
            }
            catch (Exception ex)
            {
                Log.Error("TestService :: Exception in AtsWmsTaskConfirmationServiceTaskThread :: " + ex.Message);
            }


        }

        protected override void OnStop()
        {


            try
            {
                Log.Debug("OnStop :: AtsWmsTaskConfirmationService in OnStop ends..!!");
            }
            catch (Exception ex)
            {
                Log.Error("OnStop :: Exception occured in OnStop :: " + ex.Message);
            }
        }
    }
}
