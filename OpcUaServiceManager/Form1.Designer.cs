namespace OpcUaServiceManager
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(Form1));
            this.btnSvcCreate = new System.Windows.Forms.Button();
            this.listBoxOpcUaServices = new System.Windows.Forms.ListBox();
            this.lblOpcUaExtractorFound = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.txtSvcName = new System.Windows.Forms.TextBox();
            this.txtSvcDescription = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.folderBrowserDialog1 = new System.Windows.Forms.FolderBrowserDialog();
            this.txtSvcFolder = new System.Windows.Forms.TextBox();
            this.btnSelectSvcFolder = new System.Windows.Forms.Button();
            this.lblCmdRunStatus = new System.Windows.Forms.Label();
            this.btnDeleteService = new System.Windows.Forms.Button();
            this.lblServices = new System.Windows.Forms.Label();
            this.SuspendLayout();
            // 
            // btnSvcCreate
            // 
            this.btnSvcCreate.Location = new System.Drawing.Point(556, 255);
            this.btnSvcCreate.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.btnSvcCreate.Name = "btnSvcCreate";
            this.btnSvcCreate.Size = new System.Drawing.Size(148, 35);
            this.btnSvcCreate.TabIndex = 0;
            this.btnSvcCreate.Text = "Create Service";
            this.btnSvcCreate.UseVisualStyleBackColor = true;
            this.btnSvcCreate.Click += new System.EventHandler(this.BtnSvcCreate_Click);
            // 
            // listBoxOpcUaServices
            // 
            this.listBoxOpcUaServices.FormattingEnabled = true;
            this.listBoxOpcUaServices.ItemHeight = 20;
            this.listBoxOpcUaServices.Location = new System.Drawing.Point(18, 38);
            this.listBoxOpcUaServices.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.listBoxOpcUaServices.Name = "listBoxOpcUaServices";
            this.listBoxOpcUaServices.Size = new System.Drawing.Size(190, 204);
            this.listBoxOpcUaServices.TabIndex = 1;
            // 
            // lblOpcUaExtractorFound
            // 
            this.lblOpcUaExtractorFound.AutoSize = true;
            this.lblOpcUaExtractorFound.Location = new System.Drawing.Point(219, 18);
            this.lblOpcUaExtractorFound.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblOpcUaExtractorFound.Name = "lblOpcUaExtractorFound";
            this.lblOpcUaExtractorFound.Size = new System.Drawing.Size(122, 20);
            this.lblOpcUaExtractorFound.TabIndex = 2;
            this.lblOpcUaExtractorFound.Text = "opcuaexestatus";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(219, 58);
            this.label1.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(147, 20);
            this.label1.TabIndex = 3;
            this.label1.Text = "Create new service:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(219, 98);
            this.label2.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(51, 20);
            this.label2.TabIndex = 4;
            this.label2.Text = "Name";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(219, 140);
            this.label3.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(89, 20);
            this.label3.TabIndex = 5;
            this.label3.Text = "Description";
            // 
            // txtSvcName
            // 
            this.txtSvcName.Location = new System.Drawing.Point(340, 94);
            this.txtSvcName.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txtSvcName.Name = "txtSvcName";
            this.txtSvcName.Size = new System.Drawing.Size(362, 26);
            this.txtSvcName.TabIndex = 7;
            // 
            // txtSvcDescription
            // 
            this.txtSvcDescription.Location = new System.Drawing.Point(340, 134);
            this.txtSvcDescription.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txtSvcDescription.Multiline = true;
            this.txtSvcDescription.Name = "txtSvcDescription";
            this.txtSvcDescription.Size = new System.Drawing.Size(362, 66);
            this.txtSvcDescription.TabIndex = 8;
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(219, 218);
            this.label4.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(95, 20);
            this.label4.TabIndex = 9;
            this.label4.Text = "Working Dir:";
            // 
            // txtSvcFolder
            // 
            this.txtSvcFolder.Location = new System.Drawing.Point(340, 214);
            this.txtSvcFolder.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.txtSvcFolder.Name = "txtSvcFolder";
            this.txtSvcFolder.Size = new System.Drawing.Size(276, 26);
            this.txtSvcFolder.TabIndex = 10;
            // 
            // btnSelectSvcFolder
            // 
            this.btnSelectSvcFolder.Location = new System.Drawing.Point(627, 211);
            this.btnSelectSvcFolder.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.btnSelectSvcFolder.Name = "btnSelectSvcFolder";
            this.btnSelectSvcFolder.Size = new System.Drawing.Size(78, 35);
            this.btnSelectSvcFolder.TabIndex = 11;
            this.btnSelectSvcFolder.Text = "Select";
            this.btnSelectSvcFolder.UseVisualStyleBackColor = true;
            this.btnSelectSvcFolder.Click += new System.EventHandler(this.BtnSelectSvcFolder_Click);
            // 
            // lblCmdRunStatus
            // 
            this.lblCmdRunStatus.AutoSize = true;
            this.lblCmdRunStatus.Location = new System.Drawing.Point(219, 263);
            this.lblCmdRunStatus.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblCmdRunStatus.Name = "lblCmdRunStatus";
            this.lblCmdRunStatus.Size = new System.Drawing.Size(127, 20);
            this.lblCmdRunStatus.TabIndex = 12;
            this.lblCmdRunStatus.Text = "command status";
            // 
            // btnDeleteService
            // 
            this.btnDeleteService.Location = new System.Drawing.Point(20, 255);
            this.btnDeleteService.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.btnDeleteService.Name = "btnDeleteService";
            this.btnDeleteService.Size = new System.Drawing.Size(136, 35);
            this.btnDeleteService.TabIndex = 13;
            this.btnDeleteService.Text = "Delete Service";
            this.btnDeleteService.UseVisualStyleBackColor = true;
            this.btnDeleteService.Click += new System.EventHandler(this.BtnDeleteService_Click);
            // 
            // lblServices
            // 
            this.lblServices.AutoSize = true;
            this.lblServices.Location = new System.Drawing.Point(18, 18);
            this.lblServices.Margin = new System.Windows.Forms.Padding(4, 0, 4, 0);
            this.lblServices.Name = "lblServices";
            this.lblServices.Size = new System.Drawing.Size(141, 20);
            this.lblServices.TabIndex = 14;
            this.lblServices.Text = "Extractor Services:";
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(9F, 20F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(714, 295);
            this.Controls.Add(this.lblServices);
            this.Controls.Add(this.btnDeleteService);
            this.Controls.Add(this.lblCmdRunStatus);
            this.Controls.Add(this.btnSelectSvcFolder);
            this.Controls.Add(this.txtSvcFolder);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.txtSvcDescription);
            this.Controls.Add(this.txtSvcName);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.lblOpcUaExtractorFound);
            this.Controls.Add(this.listBoxOpcUaServices);
            this.Controls.Add(this.btnSvcCreate);
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.Fixed3D;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.Margin = new System.Windows.Forms.Padding(4, 5, 4, 5);
            this.MaximumSize = new System.Drawing.Size(740, 355);
            this.Name = "Form1";
            this.Text = "OpcUa Service Manager";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnSvcCreate;
        private System.Windows.Forms.ListBox listBoxOpcUaServices;
        private System.Windows.Forms.Label lblOpcUaExtractorFound;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtSvcName;
        private System.Windows.Forms.TextBox txtSvcDescription;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.FolderBrowserDialog folderBrowserDialog1;
        private System.Windows.Forms.TextBox txtSvcFolder;
        private System.Windows.Forms.Button btnSelectSvcFolder;
        private System.Windows.Forms.Label lblCmdRunStatus;
        private System.Windows.Forms.Button btnDeleteService;
        private System.Windows.Forms.Label lblServices;
    }
}

