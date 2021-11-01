using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Management;
using System.ServiceProcess;
using System.Windows.Forms;

namespace OpcUaServiceManager
{
    public partial class Form1 : Form
    {
        /// <summary>
        /// Private vars
        /// </summary>
        private const string _serviceBaseName = "opcuaext";
        private ServiceController[] _winServices;
        private List<ServiceController> _cogniteServices;
        private List<string> _serviceNamesInUse;
        private readonly string _opcuaExtractorDir;
        private readonly string _opcuaExtractorExe = @"OpcUaExtractor\bin\OpcuaExtractor.exe";
        private readonly bool _opcuaExtractorCanCreateService = false;
        private int _nextServiceNumber;

        /// <summary>
        /// Initialize the form, and validate OPCUA extractor install and that we have the service .exe file
        /// </summary>
        public Form1()
        {
            InitializeComponent();

            // Set some default values
            lblOpcUaExtractorFound.Text = "OpcUaService executeable not found.";
            lblOpcUaExtractorFound.ForeColor = Color.Red;
            lblCmdRunStatus.Text = "";
            lblServices.ForeColor = Color.Green;

            GetOpcUaExtractorServices();

            // Get installdir from registry, created by the installer.
            RegistryKey key = Registry.LocalMachine.OpenSubKey(@"Software\Cognite\OpcUaExtractor");

            if (key != null)
            {
                _opcuaExtractorDir = key.GetValue("InstallFolder").ToString();
            }

            if (File.Exists(_opcuaExtractorDir + _opcuaExtractorExe))
            {
                lblOpcUaExtractorFound.Text = "OpcUaService found: " + _opcuaExtractorDir + _opcuaExtractorExe;
                lblOpcUaExtractorFound.ForeColor = Color.Green;
                _opcuaExtractorCanCreateService = true;
            }
        }

        /// <summary>
        /// Creates a new OpcUa extractor service
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void BtnSvcCreate_Click(object sender, EventArgs e)
        {
            lblCmdRunStatus.Text = "";

            txtSvcDescription.Text = txtSvcDescription.Text.Replace(Environment.NewLine, " ");
            txtSvcFolder.Text = txtSvcFolder.Text.Trim();
            txtSvcName.Text = txtSvcName.Text.Trim();

            if (!_opcuaExtractorCanCreateService)
            {
                MessageBox.Show("OpcUa Extractor setup is wrong. Operation cancelled.");
                return;
            }

            if (string.IsNullOrWhiteSpace(txtSvcName.Text))
            {
                MessageBox.Show("You need to provide a name for the service.");
                return;
            }

            var duplicateCheck = _serviceNamesInUse.FirstOrDefault(x => x.Contains(txtSvcName.Text.ToLower()));
            if (duplicateCheck != null)
            {
                MessageBox.Show("The name specified is already in use.");
                return;
            }

            if (!Directory.Exists(txtSvcFolder.Text))
            {
                MessageBox.Show("Folder specified does not exist.");
                return;
            }

            string cmd = string.Format(@"/C sc create {2} binPath= ""\""{0}\"" -s -w \""{1}\"" "" DisplayName= ""{3}""",
                _opcuaExtractorDir + _opcuaExtractorExe, txtSvcFolder.Text, _serviceBaseName + _nextServiceNumber, txtSvcName.Text);
            string result = RunCommand.Run(cmd);
            if (result.Contains("SUCCESS"))
            {
                RunCommand.Run(string.Format(@"/C sc description {0} ""{1}""", _serviceBaseName + _nextServiceNumber, txtSvcDescription.Text));
                result = result.Replace("[SC] ", "");
                lblCmdRunStatus.Text = result;
                lblCmdRunStatus.ForeColor = Color.Green;
            }
            else
            {
                lblCmdRunStatus.Text = "Failed to create service.";
                lblCmdRunStatus.ForeColor = Color.Red;
            }

            GetOpcUaExtractorServices();

        }

        /// <summary>
        /// Opens a folder select dialog, for use as working dir for the extractor service
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void BtnSelectSvcFolder_Click(object sender, EventArgs e)
        {
            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
            {
                txtSvcFolder.Text = folderBrowserDialog1.SelectedPath;
            }
        }

        /// <summary>
        /// Deletes the selected windows service
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void BtnDeleteService_Click(object sender, EventArgs e)
        {
            lblCmdRunStatus.Text = "";
            ServiceController svcSelected = (ServiceController)listBoxOpcUaServices.SelectedItem;

            if (svcSelected == null)
            {
                lblCmdRunStatus.Text = "No service to delete.";
                lblCmdRunStatus.ForeColor = Color.Red;
                return;
            }

            DialogResult userCheck = MessageBox.Show("Are you sure you want to delete this service ?", "Confirm Delete", MessageBoxButtons.YesNo, MessageBoxIcon.Warning);

            if (userCheck == DialogResult.Yes)
            {
                string result = RunCommand.Run(string.Format(@"/C sc delete {0}", svcSelected.ServiceName));
                if (result.Contains("SUCCESS"))
                {
                    result = result.Replace("[SC] ", "");
                    lblCmdRunStatus.Text = result;
                    lblCmdRunStatus.ForeColor = Color.Green;
                }
                else
                {
                    lblCmdRunStatus.Text = "Failed to delete service.";
                    lblCmdRunStatus.ForeColor = Color.Red;
                }

                GetOpcUaExtractorServices();
            }

        }

        /// <summary>
        /// Finds and updates list of all opcua extractor services this tool can control 
        /// </summary>
        private void GetOpcUaExtractorServices()
        {
            // Get all windows services, and create a list of our custom opcua extractor services.
            _winServices = ServiceController.GetServices();
            _cogniteServices = new List<ServiceController>();
            _serviceNamesInUse = new List<string>();
            _nextServiceNumber = 1;

            List<int> serviceNumbersUsed = new List<int>();

            foreach (ServiceController sc in _winServices)
            {
                _serviceNamesInUse.Add(sc.DisplayName.ToLower());

                if (sc.ServiceName.StartsWith(_serviceBaseName))
                {
                    _cogniteServices.Add(sc);

                    string num = sc.ServiceName.Replace(_serviceBaseName, "");
                    if (!string.IsNullOrWhiteSpace(num))
                    {
                        serviceNumbersUsed.Add(Convert.ToInt32(num));
                    }
                }
            }

            if (serviceNumbersUsed != null && serviceNumbersUsed.Count > 0)
            {
                _nextServiceNumber = serviceNumbersUsed.Max() + 1;
            }

            // Show our services already made in the listbox
            listBoxOpcUaServices.DataSource = _cogniteServices;
            listBoxOpcUaServices.DisplayMember = "DisplayName";

        }

        private void ListBoxOpcUaServices_SelectedIndexChanged(object sender, EventArgs e)
        {
            ServiceController service = (ServiceController)listBoxOpcUaServices.SelectedItem;

            var serviceObject = new ManagementObject(new ManagementPath(string.Format("Win32_Service.Name='{0}'", service.ServiceName)));
            var path = serviceObject["PathName"]?.ToString();
            var description = serviceObject["Description"]?.ToString();

            if (!string.IsNullOrWhiteSpace(path))
            {
                var pathItems = path.Split(' ');
                path = (!string.IsNullOrWhiteSpace(pathItems[1])) ? pathItems[1].Replace(@"""", "") : "";
            }

            txtSvcName.Text = service.DisplayName;
            txtSvcDescription.Text = (!string.IsNullOrWhiteSpace(description)) ? description : "";
            txtSvcFolder.Text = path;
        }

    }
}
