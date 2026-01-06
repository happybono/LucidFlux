namespace BeamPositionStability
{
    partial class FrmMain
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
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FrmMain));
            this.lvPoints = new System.Windows.Forms.ListView();
            this.Index = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.Timestamp = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.X = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.Y = ((System.Windows.Forms.ColumnHeader)(new System.Windows.Forms.ColumnHeader()));
            this.lblCentroidX = new System.Windows.Forms.Label();
            this.lblCentroidY = new System.Windows.Forms.Label();
            this.lblAzimuth = new System.Windows.Forms.Label();
            this.lblDeltaX = new System.Windows.Forms.Label();
            this.lblDeltaY = new System.Windows.Forms.Label();
            this.lblDelta = new System.Windows.Forms.Label();
            this.gbUnits = new System.Windows.Forms.GroupBox();
            this.rbtnRad = new System.Windows.Forms.RadioButton();
            this.rbtnDeg = new System.Windows.Forms.RadioButton();
            this.lblTotal = new System.Windows.Forms.Label();
            this.lblConsidered = new System.Windows.Forms.Label();
            this.lblTCentroidX = new System.Windows.Forms.Label();
            this.lblTCentroidY = new System.Windows.Forms.Label();
            this.lblTAzimuth = new System.Windows.Forms.Label();
            this.lblTDeltaX = new System.Windows.Forms.Label();
            this.lblTDeltaY = new System.Windows.Forms.Label();
            this.lblTDelta = new System.Windows.Forms.Label();
            this.lblTConsidered = new System.Windows.Forms.Label();
            this.lblTCount = new System.Windows.Forms.Label();
            this.gbSettings = new System.Windows.Forms.GroupBox();
            this.lblValues = new System.Windows.Forms.Label();
            this.lblCustomSeqNumber = new System.Windows.Forms.TextBox();
            this.rbtnCustomSeq = new System.Windows.Forms.RadioButton();
            this.rbtn1000Values = new System.Windows.Forms.RadioButton();
            this.rbtnAllValues = new System.Windows.Forms.RadioButton();
            this.lblSeconds = new System.Windows.Forms.Label();
            this.txtCustomTime = new System.Windows.Forms.TextBox();
            this.rbtnCustomTime = new System.Windows.Forms.RadioButton();
            this.rbtnLongTerm = new System.Windows.Forms.RadioButton();
            this.rbtnMidTerm = new System.Windows.Forms.RadioButton();
            this.rbtnShortTerm = new System.Windows.Forms.RadioButton();
            this.chkExportConsideredOnly = new System.Windows.Forms.CheckBox();
            this.chkOpenBeforeSave = new System.Windows.Forms.CheckBox();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openCSVToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.exportAsExcelToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openCSVDialog = new System.Windows.Forms.OpenFileDialog();
            this.btnExport = new System.Windows.Forms.Button();
            this.btnClear = new System.Windows.Forms.Button();
            this.gbSaveSettings = new System.Windows.Forms.GroupBox();
            this.pbMain = new System.Windows.Forms.ProgressBar();
            this.statStripMain = new System.Windows.Forms.StatusStrip();
            this.slblDesc = new System.Windows.Forms.ToolStripStatusLabel();
            this.gbUnits.SuspendLayout();
            this.gbSettings.SuspendLayout();
            this.groupBox1.SuspendLayout();
            this.menuStrip1.SuspendLayout();
            this.gbSaveSettings.SuspendLayout();
            this.statStripMain.SuspendLayout();
            this.SuspendLayout();
            // 
            // lvPoints
            // 
            this.lvPoints.AllowDrop = true;
            this.lvPoints.AutoArrange = false;
            this.lvPoints.Columns.AddRange(new System.Windows.Forms.ColumnHeader[] {
            this.Index,
            this.Timestamp,
            this.X,
            this.Y});
            this.lvPoints.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lvPoints.FullRowSelect = true;
            this.lvPoints.GridLines = true;
            this.lvPoints.HideSelection = false;
            this.lvPoints.Location = new System.Drawing.Point(29, 47);
            this.lvPoints.Name = "lvPoints";
            this.lvPoints.Size = new System.Drawing.Size(440, 577);
            this.lvPoints.TabIndex = 0;
            this.lvPoints.UseCompatibleStateImageBehavior = false;
            this.lvPoints.View = System.Windows.Forms.View.Details;
            this.lvPoints.DragDrop += new System.Windows.Forms.DragEventHandler(this.lvPoints_DragDrop);
            this.lvPoints.DragEnter += new System.Windows.Forms.DragEventHandler(this.lvPoints_DragEnter);
            this.lvPoints.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lvPoints_KeyDown);
            this.lvPoints.MouseLeave += new System.EventHandler(this.lvPoints_MouseLeave);
            this.lvPoints.MouseHover += new System.EventHandler(this.lvPoints_MouseHover);
            // 
            // Index
            // 
            this.Index.Text = "Index";
            // 
            // Timestamp
            // 
            this.Timestamp.Text = "Timestamp";
            // 
            // X
            // 
            this.X.Text = "X";
            // 
            // Y
            // 
            this.Y.Text = "Y";
            // 
            // lblCentroidX
            // 
            this.lblCentroidX.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblCentroidX.Location = new System.Drawing.Point(243, 23);
            this.lblCentroidX.Name = "lblCentroidX";
            this.lblCentroidX.Size = new System.Drawing.Size(150, 17);
            this.lblCentroidX.TabIndex = 1;
            this.lblCentroidX.Text = "--";
            this.lblCentroidX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblCentroidX.MouseLeave += new System.EventHandler(this.lblCentroidX_MouseLeave);
            this.lblCentroidX.MouseHover += new System.EventHandler(this.lblCentroidX_MouseHover);
            // 
            // lblCentroidY
            // 
            this.lblCentroidY.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblCentroidY.Location = new System.Drawing.Point(243, 50);
            this.lblCentroidY.Name = "lblCentroidY";
            this.lblCentroidY.Size = new System.Drawing.Size(150, 17);
            this.lblCentroidY.TabIndex = 2;
            this.lblCentroidY.Text = "--";
            this.lblCentroidY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblCentroidY.MouseLeave += new System.EventHandler(this.lblCentroidY_MouseLeave);
            this.lblCentroidY.MouseHover += new System.EventHandler(this.lblCentroidY_MouseHover);
            // 
            // lblAzimuth
            // 
            this.lblAzimuth.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblAzimuth.Location = new System.Drawing.Point(243, 88);
            this.lblAzimuth.Name = "lblAzimuth";
            this.lblAzimuth.Size = new System.Drawing.Size(150, 17);
            this.lblAzimuth.TabIndex = 3;
            this.lblAzimuth.Text = "--";
            this.lblAzimuth.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblAzimuth.MouseLeave += new System.EventHandler(this.lblAzimuth_MouseLeave);
            this.lblAzimuth.MouseHover += new System.EventHandler(this.lblAzimuth_MouseHover);
            // 
            // lblDeltaX
            // 
            this.lblDeltaX.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDeltaX.Location = new System.Drawing.Point(243, 115);
            this.lblDeltaX.Name = "lblDeltaX";
            this.lblDeltaX.Size = new System.Drawing.Size(150, 17);
            this.lblDeltaX.TabIndex = 4;
            this.lblDeltaX.Text = "--";
            this.lblDeltaX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblDeltaX.MouseLeave += new System.EventHandler(this.lblDeltaX_MouseLeave);
            this.lblDeltaX.MouseHover += new System.EventHandler(this.lblDeltaX_MouseHover);
            // 
            // lblDeltaY
            // 
            this.lblDeltaY.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDeltaY.Location = new System.Drawing.Point(243, 142);
            this.lblDeltaY.Name = "lblDeltaY";
            this.lblDeltaY.Size = new System.Drawing.Size(150, 17);
            this.lblDeltaY.TabIndex = 5;
            this.lblDeltaY.Text = "--";
            this.lblDeltaY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblDeltaY.MouseLeave += new System.EventHandler(this.lblDeltaY_MouseLeave);
            this.lblDeltaY.MouseHover += new System.EventHandler(this.lblDeltaY_MouseHover);
            // 
            // lblDelta
            // 
            this.lblDelta.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDelta.Location = new System.Drawing.Point(243, 169);
            this.lblDelta.Name = "lblDelta";
            this.lblDelta.Size = new System.Drawing.Size(150, 17);
            this.lblDelta.TabIndex = 6;
            this.lblDelta.Text = "--";
            this.lblDelta.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblDelta.MouseLeave += new System.EventHandler(this.lblDelta_MouseLeave);
            this.lblDelta.MouseHover += new System.EventHandler(this.lblDelta_MouseHover);
            // 
            // gbUnits
            // 
            this.gbUnits.Controls.Add(this.rbtnRad);
            this.gbUnits.Controls.Add(this.rbtnDeg);
            this.gbUnits.Font = new System.Drawing.Font("Segoe UI Variable Display Semil", 11.25F);
            this.gbUnits.Location = new System.Drawing.Point(499, 325);
            this.gbUnits.Name = "gbUnits";
            this.gbUnits.Size = new System.Drawing.Size(470, 90);
            this.gbUnits.TabIndex = 7;
            this.gbUnits.TabStop = false;
            this.gbUnits.Text = "Units";
            // 
            // rbtnRad
            // 
            this.rbtnRad.AutoSize = true;
            this.rbtnRad.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnRad.Location = new System.Drawing.Point(246, 39);
            this.rbtnRad.Name = "rbtnRad";
            this.rbtnRad.Size = new System.Drawing.Size(70, 23);
            this.rbtnRad.TabIndex = 1;
            this.rbtnRad.TabStop = true;
            this.rbtnRad.Text = "Radian";
            this.rbtnRad.UseVisualStyleBackColor = true;
            this.rbtnRad.CheckedChanged += new System.EventHandler(this.rbtnRad_CheckedChanged);
            this.rbtnRad.MouseLeave += new System.EventHandler(this.rbtnRad_MouseLeave);
            this.rbtnRad.MouseHover += new System.EventHandler(this.rbtnRad_MouseHover);
            // 
            // rbtnDeg
            // 
            this.rbtnDeg.AutoSize = true;
            this.rbtnDeg.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnDeg.Location = new System.Drawing.Point(154, 39);
            this.rbtnDeg.Name = "rbtnDeg";
            this.rbtnDeg.Size = new System.Drawing.Size(77, 23);
            this.rbtnDeg.TabIndex = 0;
            this.rbtnDeg.TabStop = true;
            this.rbtnDeg.Text = "Degrees";
            this.rbtnDeg.UseVisualStyleBackColor = true;
            this.rbtnDeg.CheckedChanged += new System.EventHandler(this.rbtnDeg_CheckedChanged);
            this.rbtnDeg.MouseLeave += new System.EventHandler(this.rbtnDeg_MouseLeave);
            this.rbtnDeg.MouseHover += new System.EventHandler(this.rbtnDeg_MouseHover);
            // 
            // lblTotal
            // 
            this.lblTotal.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTotal.Location = new System.Drawing.Point(243, 202);
            this.lblTotal.Name = "lblTotal";
            this.lblTotal.Size = new System.Drawing.Size(150, 17);
            this.lblTotal.TabIndex = 8;
            this.lblTotal.Text = "--";
            this.lblTotal.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTotal.MouseLeave += new System.EventHandler(this.lblTotal_MouseLeave);
            this.lblTotal.MouseHover += new System.EventHandler(this.lblTotal_MouseHover);
            // 
            // lblConsidered
            // 
            this.lblConsidered.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblConsidered.Location = new System.Drawing.Point(243, 229);
            this.lblConsidered.Name = "lblConsidered";
            this.lblConsidered.Size = new System.Drawing.Size(150, 17);
            this.lblConsidered.TabIndex = 9;
            this.lblConsidered.Text = "--";
            this.lblConsidered.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblConsidered.MouseLeave += new System.EventHandler(this.lblConsidered_MouseLeave);
            this.lblConsidered.MouseHover += new System.EventHandler(this.lblConsidered_MouseHover);
            // 
            // lblTCentroidX
            // 
            this.lblTCentroidX.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTCentroidX.Location = new System.Drawing.Point(78, 23);
            this.lblTCentroidX.Name = "lblTCentroidX";
            this.lblTCentroidX.Size = new System.Drawing.Size(150, 17);
            this.lblTCentroidX.TabIndex = 10;
            this.lblTCentroidX.Text = "CentroidX";
            this.lblTCentroidX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTCentroidX.MouseLeave += new System.EventHandler(this.lblTCentroidX_MouseLeave);
            this.lblTCentroidX.MouseHover += new System.EventHandler(this.lblTCentroidX_MouseHover);
            // 
            // lblTCentroidY
            // 
            this.lblTCentroidY.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTCentroidY.Location = new System.Drawing.Point(78, 50);
            this.lblTCentroidY.Name = "lblTCentroidY";
            this.lblTCentroidY.Size = new System.Drawing.Size(150, 17);
            this.lblTCentroidY.TabIndex = 11;
            this.lblTCentroidY.Text = "CentroidY";
            this.lblTCentroidY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTCentroidY.MouseLeave += new System.EventHandler(this.lblTCentroidY_MouseLeave);
            this.lblTCentroidY.MouseHover += new System.EventHandler(this.lblTCentroidY_MouseHover);
            // 
            // lblTAzimuth
            // 
            this.lblTAzimuth.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTAzimuth.Location = new System.Drawing.Point(78, 88);
            this.lblTAzimuth.Name = "lblTAzimuth";
            this.lblTAzimuth.Size = new System.Drawing.Size(150, 17);
            this.lblTAzimuth.TabIndex = 12;
            this.lblTAzimuth.Text = "Azimuth";
            this.lblTAzimuth.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTAzimuth.MouseLeave += new System.EventHandler(this.lblTAzimuth_MouseLeave);
            this.lblTAzimuth.MouseHover += new System.EventHandler(this.lblTAzimuth_MouseHover);
            // 
            // lblTDeltaX
            // 
            this.lblTDeltaX.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTDeltaX.Location = new System.Drawing.Point(78, 115);
            this.lblTDeltaX.Name = "lblTDeltaX";
            this.lblTDeltaX.Size = new System.Drawing.Size(150, 17);
            this.lblTDeltaX.TabIndex = 13;
            this.lblTDeltaX.Text = "DeltaX";
            this.lblTDeltaX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTDeltaX.MouseLeave += new System.EventHandler(this.lblTDeltaX_MouseLeave);
            this.lblTDeltaX.MouseHover += new System.EventHandler(this.lblTDeltaX_MouseHover);
            // 
            // lblTDeltaY
            // 
            this.lblTDeltaY.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTDeltaY.Location = new System.Drawing.Point(78, 142);
            this.lblTDeltaY.Name = "lblTDeltaY";
            this.lblTDeltaY.Size = new System.Drawing.Size(150, 17);
            this.lblTDeltaY.TabIndex = 14;
            this.lblTDeltaY.Text = "DeltaY";
            this.lblTDeltaY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTDeltaY.MouseLeave += new System.EventHandler(this.lblTDeltaY_MouseLeave);
            this.lblTDeltaY.MouseHover += new System.EventHandler(this.lblTDeltaY_MouseHover);
            // 
            // lblTDelta
            // 
            this.lblTDelta.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTDelta.Location = new System.Drawing.Point(78, 169);
            this.lblTDelta.Name = "lblTDelta";
            this.lblTDelta.Size = new System.Drawing.Size(150, 17);
            this.lblTDelta.TabIndex = 15;
            this.lblTDelta.Text = "Delta";
            this.lblTDelta.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTDelta.MouseLeave += new System.EventHandler(this.lblTDelta_MouseLeave);
            this.lblTDelta.MouseHover += new System.EventHandler(this.lblTDelta_MouseHover);
            // 
            // lblTConsidered
            // 
            this.lblTConsidered.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTConsidered.Location = new System.Drawing.Point(78, 229);
            this.lblTConsidered.Name = "lblTConsidered";
            this.lblTConsidered.Size = new System.Drawing.Size(150, 17);
            this.lblTConsidered.TabIndex = 16;
            this.lblTConsidered.Text = "Count (Considered)";
            this.lblTConsidered.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTConsidered.MouseLeave += new System.EventHandler(this.lblTConsidered_MouseLeave);
            this.lblTConsidered.MouseHover += new System.EventHandler(this.lblTConsidered_MouseHover);
            // 
            // lblTCount
            // 
            this.lblTCount.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTCount.Location = new System.Drawing.Point(78, 202);
            this.lblTCount.Name = "lblTCount";
            this.lblTCount.Size = new System.Drawing.Size(150, 17);
            this.lblTCount.TabIndex = 17;
            this.lblTCount.Text = "Count";
            this.lblTCount.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            this.lblTCount.MouseLeave += new System.EventHandler(this.lblTCount_MouseLeave);
            this.lblTCount.MouseHover += new System.EventHandler(this.lblTCount_MouseHover);
            // 
            // gbSettings
            // 
            this.gbSettings.Controls.Add(this.lblValues);
            this.gbSettings.Controls.Add(this.lblCustomSeqNumber);
            this.gbSettings.Controls.Add(this.rbtnCustomSeq);
            this.gbSettings.Controls.Add(this.rbtn1000Values);
            this.gbSettings.Controls.Add(this.rbtnAllValues);
            this.gbSettings.Controls.Add(this.lblSeconds);
            this.gbSettings.Controls.Add(this.txtCustomTime);
            this.gbSettings.Controls.Add(this.rbtnCustomTime);
            this.gbSettings.Controls.Add(this.rbtnLongTerm);
            this.gbSettings.Controls.Add(this.rbtnMidTerm);
            this.gbSettings.Controls.Add(this.rbtnShortTerm);
            this.gbSettings.Font = new System.Drawing.Font("Segoe UI Variable Display Semil", 11.25F);
            this.gbSettings.Location = new System.Drawing.Point(499, 424);
            this.gbSettings.Name = "gbSettings";
            this.gbSettings.Size = new System.Drawing.Size(470, 191);
            this.gbSettings.TabIndex = 8;
            this.gbSettings.TabStop = false;
            this.gbSettings.Text = "Settings";
            // 
            // lblValues
            // 
            this.lblValues.AutoSize = true;
            this.lblValues.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.lblValues.Location = new System.Drawing.Point(350, 126);
            this.lblValues.Name = "lblValues";
            this.lblValues.Size = new System.Drawing.Size(58, 19);
            this.lblValues.TabIndex = 10;
            this.lblValues.Text = "value(s)";
            this.lblValues.MouseLeave += new System.EventHandler(this.lblValues_MouseLeave);
            this.lblValues.MouseHover += new System.EventHandler(this.lblValues_MouseHover);
            // 
            // lblCustomSeqNumber
            // 
            this.lblCustomSeqNumber.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.lblCustomSeqNumber.Location = new System.Drawing.Point(241, 123);
            this.lblCustomSeqNumber.Name = "lblCustomSeqNumber";
            this.lblCustomSeqNumber.Size = new System.Drawing.Size(100, 25);
            this.lblCustomSeqNumber.TabIndex = 9;
            this.lblCustomSeqNumber.TextChanged += new System.EventHandler(this.lblCustomSeqNumber_TextChanged);
            this.lblCustomSeqNumber.MouseLeave += new System.EventHandler(this.lblCustomSeqNumber_MouseLeave);
            this.lblCustomSeqNumber.MouseHover += new System.EventHandler(this.lblCustomSeqNumber_MouseHover);
            // 
            // rbtnCustomSeq
            // 
            this.rbtnCustomSeq.AutoSize = true;
            this.rbtnCustomSeq.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnCustomSeq.Location = new System.Drawing.Point(241, 91);
            this.rbtnCustomSeq.Name = "rbtnCustomSeq";
            this.rbtnCustomSeq.Size = new System.Drawing.Size(158, 23);
            this.rbtnCustomSeq.TabIndex = 8;
            this.rbtnCustomSeq.TabStop = true;
            this.rbtnCustomSeq.Text = "Self defined number :";
            this.rbtnCustomSeq.UseVisualStyleBackColor = true;
            this.rbtnCustomSeq.CheckedChanged += new System.EventHandler(this.rbtnCustomSeq_CheckedChanged);
            this.rbtnCustomSeq.MouseLeave += new System.EventHandler(this.rbtnCustomSeq_MouseLeave);
            this.rbtnCustomSeq.MouseHover += new System.EventHandler(this.rbtnCustomSeq_MouseHover);
            // 
            // rbtn1000Values
            // 
            this.rbtn1000Values.AutoSize = true;
            this.rbtn1000Values.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtn1000Values.Location = new System.Drawing.Point(241, 64);
            this.rbtn1000Values.Name = "rbtn1000Values";
            this.rbtn1000Values.Size = new System.Drawing.Size(223, 23);
            this.rbtn1000Values.TabIndex = 7;
            this.rbtn1000Values.TabStop = true;
            this.rbtn1000Values.Text = "Last 1000 Values (ISO Standard)";
            this.rbtn1000Values.UseVisualStyleBackColor = true;
            this.rbtn1000Values.CheckedChanged += new System.EventHandler(this.rbtn1000Values_CheckedChanged);
            this.rbtn1000Values.MouseLeave += new System.EventHandler(this.rbtn1000Values_MouseLeave);
            this.rbtn1000Values.MouseHover += new System.EventHandler(this.rbtn1000Values_MouseHover);
            // 
            // rbtnAllValues
            // 
            this.rbtnAllValues.AutoSize = true;
            this.rbtnAllValues.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnAllValues.Location = new System.Drawing.Point(241, 37);
            this.rbtnAllValues.Name = "rbtnAllValues";
            this.rbtnAllValues.Size = new System.Drawing.Size(160, 23);
            this.rbtnAllValues.TabIndex = 6;
            this.rbtnAllValues.TabStop = true;
            this.rbtnAllValues.Text = "All determined values";
            this.rbtnAllValues.UseVisualStyleBackColor = true;
            this.rbtnAllValues.CheckedChanged += new System.EventHandler(this.rbtnAllValues_CheckedChanged);
            this.rbtnAllValues.MouseLeave += new System.EventHandler(this.rbtnAllValues_MouseLeave);
            this.rbtnAllValues.MouseHover += new System.EventHandler(this.rbtnAllValues_MouseHover);
            // 
            // lblSeconds
            // 
            this.lblSeconds.AutoSize = true;
            this.lblSeconds.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.lblSeconds.Location = new System.Drawing.Point(139, 154);
            this.lblSeconds.Name = "lblSeconds";
            this.lblSeconds.Size = new System.Drawing.Size(69, 19);
            this.lblSeconds.TabIndex = 5;
            this.lblSeconds.Text = "second(s)";
            this.lblSeconds.MouseLeave += new System.EventHandler(this.lblSeconds_MouseLeave);
            this.lblSeconds.MouseHover += new System.EventHandler(this.lblSeconds_MouseHover);
            // 
            // txtCustomTime
            // 
            this.txtCustomTime.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.txtCustomTime.Location = new System.Drawing.Point(30, 151);
            this.txtCustomTime.Name = "txtCustomTime";
            this.txtCustomTime.Size = new System.Drawing.Size(100, 25);
            this.txtCustomTime.TabIndex = 4;
            this.txtCustomTime.TextChanged += new System.EventHandler(this.txtCustomTime_TextChanged);
            this.txtCustomTime.MouseLeave += new System.EventHandler(this.txtCustomTime_MouseLeave);
            this.txtCustomTime.MouseHover += new System.EventHandler(this.txtCustomTime_MouseHover);
            // 
            // rbtnCustomTime
            // 
            this.rbtnCustomTime.AutoSize = true;
            this.rbtnCustomTime.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnCustomTime.Location = new System.Drawing.Point(10, 118);
            this.rbtnCustomTime.Name = "rbtnCustomTime";
            this.rbtnCustomTime.Size = new System.Drawing.Size(141, 23);
            this.rbtnCustomTime.TabIndex = 3;
            this.rbtnCustomTime.TabStop = true;
            this.rbtnCustomTime.Text = "Self defined time : ";
            this.rbtnCustomTime.UseVisualStyleBackColor = true;
            this.rbtnCustomTime.CheckedChanged += new System.EventHandler(this.rbtnCustomTime_CheckedChanged);
            this.rbtnCustomTime.MouseLeave += new System.EventHandler(this.rbtnCustomTime_MouseLeave);
            this.rbtnCustomTime.MouseHover += new System.EventHandler(this.rbtnCustomTime_MouseHover);
            // 
            // rbtnLongTerm
            // 
            this.rbtnLongTerm.AutoSize = true;
            this.rbtnLongTerm.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnLongTerm.Location = new System.Drawing.Point(10, 91);
            this.rbtnLongTerm.Name = "rbtnLongTerm";
            this.rbtnLongTerm.Size = new System.Drawing.Size(211, 23);
            this.rbtnLongTerm.TabIndex = 2;
            this.rbtnLongTerm.TabStop = true;
            this.rbtnLongTerm.Text = "Long-term Evaluation (1 hour)";
            this.rbtnLongTerm.UseVisualStyleBackColor = true;
            this.rbtnLongTerm.CheckedChanged += new System.EventHandler(this.rbtnLongTerm_CheckedChanged);
            this.rbtnLongTerm.MouseLeave += new System.EventHandler(this.rbtnLongTerm_MouseLeave);
            this.rbtnLongTerm.MouseHover += new System.EventHandler(this.rbtnLongTerm_MouseHover);
            // 
            // rbtnMidTerm
            // 
            this.rbtnMidTerm.AutoSize = true;
            this.rbtnMidTerm.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnMidTerm.Location = new System.Drawing.Point(10, 64);
            this.rbtnMidTerm.Name = "rbtnMidTerm";
            this.rbtnMidTerm.Size = new System.Drawing.Size(220, 23);
            this.rbtnMidTerm.TabIndex = 1;
            this.rbtnMidTerm.TabStop = true;
            this.rbtnMidTerm.Text = "Mid-term Evaluation (1 minute)";
            this.rbtnMidTerm.UseVisualStyleBackColor = true;
            this.rbtnMidTerm.CheckedChanged += new System.EventHandler(this.rbtnMidTerm_CheckedChanged);
            this.rbtnMidTerm.MouseLeave += new System.EventHandler(this.rbtnMidTerm_MouseLeave);
            this.rbtnMidTerm.MouseHover += new System.EventHandler(this.rbtnMidTerm_MouseHover);
            // 
            // rbtnShortTerm
            // 
            this.rbtnShortTerm.AutoSize = true;
            this.rbtnShortTerm.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 10F);
            this.rbtnShortTerm.Location = new System.Drawing.Point(10, 37);
            this.rbtnShortTerm.Name = "rbtnShortTerm";
            this.rbtnShortTerm.Size = new System.Drawing.Size(229, 23);
            this.rbtnShortTerm.TabIndex = 0;
            this.rbtnShortTerm.TabStop = true;
            this.rbtnShortTerm.Text = "Short-term Evaluation (1 second)";
            this.rbtnShortTerm.UseVisualStyleBackColor = true;
            this.rbtnShortTerm.CheckedChanged += new System.EventHandler(this.rbtnShortTerm_CheckedChanged);
            this.rbtnShortTerm.MouseLeave += new System.EventHandler(this.rbtnShortTerm_MouseLeave);
            this.rbtnShortTerm.MouseHover += new System.EventHandler(this.rbtnShortTerm_MouseHover);
            // 
            // chkExportConsideredOnly
            // 
            this.chkExportConsideredOnly.AutoSize = true;
            this.chkExportConsideredOnly.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkExportConsideredOnly.Location = new System.Drawing.Point(252, 19);
            this.chkExportConsideredOnly.Name = "chkExportConsideredOnly";
            this.chkExportConsideredOnly.Size = new System.Drawing.Size(198, 21);
            this.chkExportConsideredOnly.TabIndex = 29;
            this.chkExportConsideredOnly.Text = "Export considered items only";
            this.chkExportConsideredOnly.UseVisualStyleBackColor = true;
            this.chkExportConsideredOnly.MouseLeave += new System.EventHandler(this.chkExportConsideredOnly_MouseLeave);
            this.chkExportConsideredOnly.MouseHover += new System.EventHandler(this.chkExportConsideredOnly_MouseHover);
            // 
            // chkOpenBeforeSave
            // 
            this.chkOpenBeforeSave.AutoSize = true;
            this.chkOpenBeforeSave.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkOpenBeforeSave.Location = new System.Drawing.Point(21, 19);
            this.chkOpenBeforeSave.Name = "chkOpenBeforeSave";
            this.chkOpenBeforeSave.Size = new System.Drawing.Size(200, 21);
            this.chkOpenBeforeSave.TabIndex = 27;
            this.chkOpenBeforeSave.Text = "Export and edit before saving";
            this.chkOpenBeforeSave.UseVisualStyleBackColor = true;
            this.chkOpenBeforeSave.MouseLeave += new System.EventHandler(this.chkOpenBeforeSave_MouseLeave);
            this.chkOpenBeforeSave.MouseHover += new System.EventHandler(this.chkOpenBeforeSave_MouseHover);
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.lblTCentroidX);
            this.groupBox1.Controls.Add(this.lblTCount);
            this.groupBox1.Controls.Add(this.lblDeltaX);
            this.groupBox1.Controls.Add(this.lblTAzimuth);
            this.groupBox1.Controls.Add(this.lblTConsidered);
            this.groupBox1.Controls.Add(this.lblTCentroidY);
            this.groupBox1.Controls.Add(this.lblAzimuth);
            this.groupBox1.Controls.Add(this.lblCentroidX);
            this.groupBox1.Controls.Add(this.lblDeltaY);
            this.groupBox1.Controls.Add(this.lblTDelta);
            this.groupBox1.Controls.Add(this.lblTDeltaX);
            this.groupBox1.Controls.Add(this.lblTotal);
            this.groupBox1.Controls.Add(this.lblCentroidY);
            this.groupBox1.Controls.Add(this.lblConsidered);
            this.groupBox1.Controls.Add(this.lblDelta);
            this.groupBox1.Controls.Add(this.lblTDeltaY);
            this.groupBox1.Font = new System.Drawing.Font("Segoe UI Variable Display Semil", 11.25F);
            this.groupBox1.Location = new System.Drawing.Point(499, 46);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(470, 270);
            this.groupBox1.TabIndex = 18;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Results";
            // 
            // menuStrip1
            // 
            this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem});
            this.menuStrip1.Location = new System.Drawing.Point(0, 0);
            this.menuStrip1.Name = "menuStrip1";
            this.menuStrip1.Size = new System.Drawing.Size(1001, 25);
            this.menuStrip1.TabIndex = 19;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.openCSVToolStripMenuItem,
            this.exportAsExcelToolStripMenuItem});
            this.fileToolStripMenuItem.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(39, 21);
            this.fileToolStripMenuItem.Text = "File";
            // 
            // openCSVToolStripMenuItem
            // 
            this.openCSVToolStripMenuItem.Name = "openCSVToolStripMenuItem";
            this.openCSVToolStripMenuItem.Size = new System.Drawing.Size(181, 22);
            this.openCSVToolStripMenuItem.Text = "Open CSV File";
            this.openCSVToolStripMenuItem.Click += new System.EventHandler(this.openCSVFileToolStripMenuItem_Click);
            this.openCSVToolStripMenuItem.MouseLeave += new System.EventHandler(this.openCSVToolStripMenuItem_MouseLeave);
            this.openCSVToolStripMenuItem.MouseHover += new System.EventHandler(this.openCSVToolStripMenuItem_MouseHover);
            // 
            // exportAsExcelToolStripMenuItem
            // 
            this.exportAsExcelToolStripMenuItem.Name = "exportAsExcelToolStripMenuItem";
            this.exportAsExcelToolStripMenuItem.Size = new System.Drawing.Size(181, 22);
            this.exportAsExcelToolStripMenuItem.Text = "Export as Excel file";
            this.exportAsExcelToolStripMenuItem.Click += new System.EventHandler(this.exportAsExcelToolStripMenuItem_Click);
            this.exportAsExcelToolStripMenuItem.MouseLeave += new System.EventHandler(this.exportAsExcelToolStripMenuItem_MouseLeave);
            this.exportAsExcelToolStripMenuItem.MouseHover += new System.EventHandler(this.exportAsExcelToolStripMenuItem_MouseHover);
            // 
            // openCSVDialog
            // 
            this.openCSVDialog.FileName = "openFileDialog1";
            // 
            // btnExport
            // 
            this.btnExport.Font = new System.Drawing.Font("Segoe Fluent Icons", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnExport.Location = new System.Drawing.Point(29, 632);
            this.btnExport.Name = "btnExport";
            this.btnExport.Size = new System.Drawing.Size(300, 30);
            this.btnExport.TabIndex = 26;
            this.btnExport.Text = "";
            this.btnExport.UseVisualStyleBackColor = true;
            this.btnExport.Click += new System.EventHandler(this.btnExport_Click);
            this.btnExport.MouseLeave += new System.EventHandler(this.btnExport_MouseLeave);
            this.btnExport.MouseHover += new System.EventHandler(this.btnExport_MouseHover);
            // 
            // btnClear
            // 
            this.btnClear.Font = new System.Drawing.Font("Segoe Fluent Icons", 12.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnClear.Location = new System.Drawing.Point(334, 632);
            this.btnClear.Margin = new System.Windows.Forms.Padding(2);
            this.btnClear.Name = "btnClear";
            this.btnClear.Size = new System.Drawing.Size(135, 30);
            this.btnClear.TabIndex = 28;
            this.btnClear.Text = "";
            this.btnClear.UseVisualStyleBackColor = true;
            this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
            this.btnClear.MouseLeave += new System.EventHandler(this.btnClear_MouseLeave);
            this.btnClear.MouseHover += new System.EventHandler(this.btnClear_MouseHover);
            // 
            // gbSaveSettings
            // 
            this.gbSaveSettings.Controls.Add(this.chkOpenBeforeSave);
            this.gbSaveSettings.Controls.Add(this.chkExportConsideredOnly);
            this.gbSaveSettings.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.gbSaveSettings.Location = new System.Drawing.Point(499, 612);
            this.gbSaveSettings.Name = "gbSaveSettings";
            this.gbSaveSettings.Size = new System.Drawing.Size(470, 50);
            this.gbSaveSettings.TabIndex = 30;
            this.gbSaveSettings.TabStop = false;
            // 
            // pbMain
            // 
            this.pbMain.Location = new System.Drawing.Point(0, 679);
            this.pbMain.Name = "pbMain";
            this.pbMain.Size = new System.Drawing.Size(1001, 5);
            this.pbMain.TabIndex = 31;
            // 
            // statStripMain
            // 
            this.statStripMain.AutoSize = false;
            this.statStripMain.BackColor = System.Drawing.Color.DarkSlateBlue;
            this.statStripMain.ImageScalingSize = new System.Drawing.Size(32, 32);
            this.statStripMain.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.slblDesc});
            this.statStripMain.Location = new System.Drawing.Point(0, 684);
            this.statStripMain.Name = "statStripMain";
            this.statStripMain.Size = new System.Drawing.Size(1001, 24);
            this.statStripMain.SizingGrip = false;
            this.statStripMain.TabIndex = 32;
            this.statStripMain.Text = "statusStrip1";
            // 
            // slblDesc
            // 
            this.slblDesc.AutoSize = false;
            this.slblDesc.Font = new System.Drawing.Font("Segoe UI Variable Display", 9F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.slblDesc.ForeColor = System.Drawing.Color.White;
            this.slblDesc.Name = "slblDesc";
            this.slblDesc.Overflow = System.Windows.Forms.ToolStripItemOverflow.Never;
            this.slblDesc.Size = new System.Drawing.Size(1000, 19);
            this.slblDesc.Text = "To start smoothing, add data to the Initial Dataset, choose a Smoothing Method, a" +
    "nd set Smoothing Parameters.";
            // 
            // FrmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(96F, 96F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Dpi;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(1001, 708);
            this.Controls.Add(this.pbMain);
            this.Controls.Add(this.statStripMain);
            this.Controls.Add(this.btnClear);
            this.Controls.Add(this.btnExport);
            this.Controls.Add(this.gbSettings);
            this.Controls.Add(this.gbUnits);
            this.Controls.Add(this.lvPoints);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.menuStrip1);
            this.Controls.Add(this.gbSaveSettings);
            this.ForeColor = System.Drawing.Color.Black;
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
            this.MainMenuStrip = this.menuStrip1;
            this.MaximizeBox = false;
            this.Name = "FrmMain";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Beam Position Stability";
            this.Load += new System.EventHandler(this.FrmMain_Load);
            this.gbUnits.ResumeLayout(false);
            this.gbUnits.PerformLayout();
            this.gbSettings.ResumeLayout(false);
            this.gbSettings.PerformLayout();
            this.groupBox1.ResumeLayout(false);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
            this.gbSaveSettings.ResumeLayout(false);
            this.gbSaveSettings.PerformLayout();
            this.statStripMain.ResumeLayout(false);
            this.statStripMain.PerformLayout();
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.ListView lvPoints;
        private System.Windows.Forms.ColumnHeader Index;
        private System.Windows.Forms.ColumnHeader X;
        private System.Windows.Forms.ColumnHeader Y;
        private System.Windows.Forms.Label lblCentroidX;
        private System.Windows.Forms.Label lblCentroidY;
        private System.Windows.Forms.Label lblAzimuth;
        private System.Windows.Forms.Label lblDeltaX;
        private System.Windows.Forms.Label lblDeltaY;
        private System.Windows.Forms.Label lblDelta;
        private System.Windows.Forms.GroupBox gbUnits;
        private System.Windows.Forms.RadioButton rbtnDeg;
        private System.Windows.Forms.RadioButton rbtnRad;
        private System.Windows.Forms.Label lblTotal;
        private System.Windows.Forms.Label lblConsidered;
        private System.Windows.Forms.Label lblTCentroidX;
        private System.Windows.Forms.Label lblTCentroidY;
        private System.Windows.Forms.Label lblTAzimuth;
        private System.Windows.Forms.Label lblTDeltaX;
        private System.Windows.Forms.Label lblTDeltaY;
        private System.Windows.Forms.Label lblTDelta;
        private System.Windows.Forms.Label lblTConsidered;
        private System.Windows.Forms.Label lblTCount;
        private System.Windows.Forms.ColumnHeader Timestamp;
        private System.Windows.Forms.GroupBox gbSettings;
        private System.Windows.Forms.RadioButton rbtnMidTerm;
        private System.Windows.Forms.RadioButton rbtnShortTerm;
        private System.Windows.Forms.Label lblSeconds;
        private System.Windows.Forms.TextBox txtCustomTime;
        private System.Windows.Forms.RadioButton rbtnCustomTime;
        private System.Windows.Forms.RadioButton rbtnLongTerm;
        private System.Windows.Forms.RadioButton rbtn1000Values;
        private System.Windows.Forms.RadioButton rbtnAllValues;
        private System.Windows.Forms.Label lblValues;
        private System.Windows.Forms.TextBox lblCustomSeqNumber;
        private System.Windows.Forms.RadioButton rbtnCustomSeq;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem openCSVToolStripMenuItem;
        private System.Windows.Forms.OpenFileDialog openCSVDialog;
        private System.Windows.Forms.Button btnExport;
        private System.Windows.Forms.CheckBox chkOpenBeforeSave;
        private System.Windows.Forms.Button btnClear;
        private System.Windows.Forms.CheckBox chkExportConsideredOnly;
        private System.Windows.Forms.ToolStripMenuItem exportAsExcelToolStripMenuItem;
        private System.Windows.Forms.GroupBox gbSaveSettings;
        private System.Windows.Forms.ProgressBar pbMain;
        private System.Windows.Forms.StatusStrip statStripMain;
        private System.Windows.Forms.ToolStripStatusLabel slblDesc;
    }
}

