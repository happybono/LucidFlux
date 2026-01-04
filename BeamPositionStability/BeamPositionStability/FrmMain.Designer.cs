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
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label3 = new System.Windows.Forms.Label();
            this.label4 = new System.Windows.Forms.Label();
            this.label5 = new System.Windows.Forms.Label();
            this.label6 = new System.Windows.Forms.Label();
            this.label7 = new System.Windows.Forms.Label();
            this.label8 = new System.Windows.Forms.Label();
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
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.menuStrip1 = new System.Windows.Forms.MenuStrip();
            this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openCSVToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
            this.openCSVDialog = new System.Windows.Forms.OpenFileDialog();
            this.btnExport = new System.Windows.Forms.Button();
            this.chkOpenBeforeSave = new System.Windows.Forms.CheckBox();
            this.btnClear = new System.Windows.Forms.Button();
            this.gbUnits.SuspendLayout();
            this.gbSettings.SuspendLayout();
            this.groupBox1.SuspendLayout();
            this.menuStrip1.SuspendLayout();
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
            this.lvPoints.Location = new System.Drawing.Point(22, 47);
            this.lvPoints.Name = "lvPoints";
            this.lvPoints.Size = new System.Drawing.Size(352, 511);
            this.lvPoints.TabIndex = 0;
            this.lvPoints.UseCompatibleStateImageBehavior = false;
            this.lvPoints.View = System.Windows.Forms.View.Details;
            this.lvPoints.DragDrop += new System.Windows.Forms.DragEventHandler(this.lvPoints_DragDrop);
            this.lvPoints.DragEnter += new System.Windows.Forms.DragEventHandler(this.lvPoints_DragEnter);
            this.lvPoints.KeyDown += new System.Windows.Forms.KeyEventHandler(this.lvPoints_KeyDown);
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
            this.lblCentroidX.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblCentroidX.Location = new System.Drawing.Point(237, 23);
            this.lblCentroidX.Name = "lblCentroidX";
            this.lblCentroidX.Size = new System.Drawing.Size(150, 17);
            this.lblCentroidX.TabIndex = 1;
            this.lblCentroidX.Text = "--";
            this.lblCentroidX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblCentroidY
            // 
            this.lblCentroidY.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblCentroidY.Location = new System.Drawing.Point(237, 50);
            this.lblCentroidY.Name = "lblCentroidY";
            this.lblCentroidY.Size = new System.Drawing.Size(150, 17);
            this.lblCentroidY.TabIndex = 2;
            this.lblCentroidY.Text = "--";
            this.lblCentroidY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblAzimuth
            // 
            this.lblAzimuth.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblAzimuth.Location = new System.Drawing.Point(237, 88);
            this.lblAzimuth.Name = "lblAzimuth";
            this.lblAzimuth.Size = new System.Drawing.Size(150, 17);
            this.lblAzimuth.TabIndex = 3;
            this.lblAzimuth.Text = "--";
            this.lblAzimuth.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblDeltaX
            // 
            this.lblDeltaX.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDeltaX.Location = new System.Drawing.Point(237, 115);
            this.lblDeltaX.Name = "lblDeltaX";
            this.lblDeltaX.Size = new System.Drawing.Size(150, 17);
            this.lblDeltaX.TabIndex = 4;
            this.lblDeltaX.Text = "--";
            this.lblDeltaX.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblDeltaY
            // 
            this.lblDeltaY.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDeltaY.Location = new System.Drawing.Point(237, 142);
            this.lblDeltaY.Name = "lblDeltaY";
            this.lblDeltaY.Size = new System.Drawing.Size(150, 17);
            this.lblDeltaY.TabIndex = 5;
            this.lblDeltaY.Text = "--";
            this.lblDeltaY.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblDelta
            // 
            this.lblDelta.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblDelta.Location = new System.Drawing.Point(237, 169);
            this.lblDelta.Name = "lblDelta";
            this.lblDelta.Size = new System.Drawing.Size(150, 17);
            this.lblDelta.TabIndex = 6;
            this.lblDelta.Text = "--";
            this.lblDelta.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // gbUnits
            // 
            this.gbUnits.Controls.Add(this.rbtnRad);
            this.gbUnits.Controls.Add(this.rbtnDeg);
            this.gbUnits.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.gbUnits.Location = new System.Drawing.Point(410, 325);
            this.gbUnits.Name = "gbUnits";
            this.gbUnits.Size = new System.Drawing.Size(459, 90);
            this.gbUnits.TabIndex = 7;
            this.gbUnits.TabStop = false;
            this.gbUnits.Text = "Units";
            // 
            // rbtnRad
            // 
            this.rbtnRad.AutoSize = true;
            this.rbtnRad.Location = new System.Drawing.Point(242, 39);
            this.rbtnRad.Name = "rbtnRad";
            this.rbtnRad.Size = new System.Drawing.Size(66, 21);
            this.rbtnRad.TabIndex = 1;
            this.rbtnRad.TabStop = true;
            this.rbtnRad.Text = "Radian";
            this.rbtnRad.UseVisualStyleBackColor = true;
            this.rbtnRad.CheckedChanged += new System.EventHandler(this.rbtnRad_CheckedChanged);
            // 
            // rbtnDeg
            // 
            this.rbtnDeg.AutoSize = true;
            this.rbtnDeg.Location = new System.Drawing.Point(150, 39);
            this.rbtnDeg.Name = "rbtnDeg";
            this.rbtnDeg.Size = new System.Drawing.Size(75, 21);
            this.rbtnDeg.TabIndex = 0;
            this.rbtnDeg.TabStop = true;
            this.rbtnDeg.Text = "Degrees";
            this.rbtnDeg.UseVisualStyleBackColor = true;
            this.rbtnDeg.CheckedChanged += new System.EventHandler(this.rbtnDeg_CheckedChanged);
            // 
            // lblTotal
            // 
            this.lblTotal.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblTotal.Location = new System.Drawing.Point(237, 202);
            this.lblTotal.Name = "lblTotal";
            this.lblTotal.Size = new System.Drawing.Size(150, 17);
            this.lblTotal.TabIndex = 8;
            this.lblTotal.Text = "--";
            this.lblTotal.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // lblConsidered
            // 
            this.lblConsidered.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lblConsidered.Location = new System.Drawing.Point(237, 229);
            this.lblConsidered.Name = "lblConsidered";
            this.lblConsidered.Size = new System.Drawing.Size(150, 17);
            this.lblConsidered.TabIndex = 9;
            this.lblConsidered.Text = "--";
            this.lblConsidered.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label1
            // 
            this.label1.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label1.Location = new System.Drawing.Point(72, 23);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(150, 17);
            this.label1.TabIndex = 10;
            this.label1.Text = "CentroidX";
            this.label1.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label2
            // 
            this.label2.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label2.Location = new System.Drawing.Point(72, 50);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(150, 17);
            this.label2.TabIndex = 11;
            this.label2.Text = "CentroidY";
            this.label2.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label3
            // 
            this.label3.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label3.Location = new System.Drawing.Point(72, 88);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(150, 17);
            this.label3.TabIndex = 12;
            this.label3.Text = "Azimuth";
            this.label3.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label4
            // 
            this.label4.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label4.Location = new System.Drawing.Point(72, 115);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(150, 17);
            this.label4.TabIndex = 13;
            this.label4.Text = "DeltaX";
            this.label4.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label5
            // 
            this.label5.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label5.Location = new System.Drawing.Point(72, 142);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(150, 17);
            this.label5.TabIndex = 14;
            this.label5.Text = "DeltaY";
            this.label5.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label6
            // 
            this.label6.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label6.Location = new System.Drawing.Point(72, 169);
            this.label6.Name = "label6";
            this.label6.Size = new System.Drawing.Size(150, 17);
            this.label6.TabIndex = 15;
            this.label6.Text = "Delta";
            this.label6.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label7
            // 
            this.label7.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label7.Location = new System.Drawing.Point(72, 229);
            this.label7.Name = "label7";
            this.label7.Size = new System.Drawing.Size(150, 17);
            this.label7.TabIndex = 16;
            this.label7.Text = "Count (Considered)";
            this.label7.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
            // 
            // label8
            // 
            this.label8.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.label8.Location = new System.Drawing.Point(72, 202);
            this.label8.Name = "label8";
            this.label8.Size = new System.Drawing.Size(150, 17);
            this.label8.TabIndex = 17;
            this.label8.Text = "Count";
            this.label8.TextAlign = System.Drawing.ContentAlignment.MiddleCenter;
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
            this.gbSettings.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.gbSettings.Location = new System.Drawing.Point(410, 424);
            this.gbSettings.Name = "gbSettings";
            this.gbSettings.Size = new System.Drawing.Size(459, 198);
            this.gbSettings.TabIndex = 8;
            this.gbSettings.TabStop = false;
            this.gbSettings.Text = "Settings";
            // 
            // lblValues
            // 
            this.lblValues.AutoSize = true;
            this.lblValues.Location = new System.Drawing.Point(348, 126);
            this.lblValues.Name = "lblValues";
            this.lblValues.Size = new System.Drawing.Size(53, 17);
            this.lblValues.TabIndex = 10;
            this.lblValues.Text = "Value(s)";
            // 
            // lblCustomSeqNumber
            // 
            this.lblCustomSeqNumber.Location = new System.Drawing.Point(239, 123);
            this.lblCustomSeqNumber.Name = "lblCustomSeqNumber";
            this.lblCustomSeqNumber.Size = new System.Drawing.Size(100, 25);
            this.lblCustomSeqNumber.TabIndex = 9;
            this.lblCustomSeqNumber.TextChanged += new System.EventHandler(this.lblCustomSeqNumber_TextChanged);
            // 
            // rbtnCustomSeq
            // 
            this.rbtnCustomSeq.AutoSize = true;
            this.rbtnCustomSeq.Location = new System.Drawing.Point(239, 91);
            this.rbtnCustomSeq.Name = "rbtnCustomSeq";
            this.rbtnCustomSeq.Size = new System.Drawing.Size(148, 21);
            this.rbtnCustomSeq.TabIndex = 8;
            this.rbtnCustomSeq.TabStop = true;
            this.rbtnCustomSeq.Text = "Self defined number :";
            this.rbtnCustomSeq.UseVisualStyleBackColor = true;
            this.rbtnCustomSeq.CheckedChanged += new System.EventHandler(this.rbtnCustomSeq_CheckedChanged);
            // 
            // rbtn1000Values
            // 
            this.rbtn1000Values.AutoSize = true;
            this.rbtn1000Values.Location = new System.Drawing.Point(239, 64);
            this.rbtn1000Values.Name = "rbtn1000Values";
            this.rbtn1000Values.Size = new System.Drawing.Size(208, 21);
            this.rbtn1000Values.TabIndex = 7;
            this.rbtn1000Values.TabStop = true;
            this.rbtn1000Values.Text = "Last 1000 Values (ISO Standard)";
            this.rbtn1000Values.UseVisualStyleBackColor = true;
            this.rbtn1000Values.CheckedChanged += new System.EventHandler(this.rbtn1000Values_CheckedChanged);
            // 
            // rbtnAllValues
            // 
            this.rbtnAllValues.AutoSize = true;
            this.rbtnAllValues.Location = new System.Drawing.Point(239, 37);
            this.rbtnAllValues.Name = "rbtnAllValues";
            this.rbtnAllValues.Size = new System.Drawing.Size(152, 21);
            this.rbtnAllValues.TabIndex = 6;
            this.rbtnAllValues.TabStop = true;
            this.rbtnAllValues.Text = "All determined values";
            this.rbtnAllValues.UseVisualStyleBackColor = true;
            this.rbtnAllValues.CheckedChanged += new System.EventHandler(this.rbtnAllValues_CheckedChanged);
            // 
            // lblSeconds
            // 
            this.lblSeconds.AutoSize = true;
            this.lblSeconds.Location = new System.Drawing.Point(140, 154);
            this.lblSeconds.Name = "lblSeconds";
            this.lblSeconds.Size = new System.Drawing.Size(65, 17);
            this.lblSeconds.TabIndex = 5;
            this.lblSeconds.Text = "second(s)";
            // 
            // txtCustomTime
            // 
            this.txtCustomTime.Location = new System.Drawing.Point(31, 151);
            this.txtCustomTime.Name = "txtCustomTime";
            this.txtCustomTime.Size = new System.Drawing.Size(100, 25);
            this.txtCustomTime.TabIndex = 4;
            this.txtCustomTime.TextChanged += new System.EventHandler(this.txtCustomTime_TextChanged);
            // 
            // rbtnCustomTime
            // 
            this.rbtnCustomTime.AutoSize = true;
            this.rbtnCustomTime.Location = new System.Drawing.Point(11, 118);
            this.rbtnCustomTime.Name = "rbtnCustomTime";
            this.rbtnCustomTime.Size = new System.Drawing.Size(132, 21);
            this.rbtnCustomTime.TabIndex = 3;
            this.rbtnCustomTime.TabStop = true;
            this.rbtnCustomTime.Text = "Self defined time : ";
            this.rbtnCustomTime.UseVisualStyleBackColor = true;
            this.rbtnCustomTime.CheckedChanged += new System.EventHandler(this.rbtnCustomTime_CheckedChanged);
            // 
            // rbtnLongTerm
            // 
            this.rbtnLongTerm.AutoSize = true;
            this.rbtnLongTerm.Location = new System.Drawing.Point(11, 91);
            this.rbtnLongTerm.Name = "rbtnLongTerm";
            this.rbtnLongTerm.Size = new System.Drawing.Size(198, 21);
            this.rbtnLongTerm.TabIndex = 2;
            this.rbtnLongTerm.TabStop = true;
            this.rbtnLongTerm.Text = "Long-term Evaluation (1 hour)";
            this.rbtnLongTerm.UseVisualStyleBackColor = true;
            this.rbtnLongTerm.CheckedChanged += new System.EventHandler(this.rbtnLongTerm_CheckedChanged);
            // 
            // rbtnMidTerm
            // 
            this.rbtnMidTerm.AutoSize = true;
            this.rbtnMidTerm.Location = new System.Drawing.Point(11, 64);
            this.rbtnMidTerm.Name = "rbtnMidTerm";
            this.rbtnMidTerm.Size = new System.Drawing.Size(186, 21);
            this.rbtnMidTerm.TabIndex = 1;
            this.rbtnMidTerm.TabStop = true;
            this.rbtnMidTerm.Text = "Mid-term Evaluation (1 min)";
            this.rbtnMidTerm.UseVisualStyleBackColor = true;
            this.rbtnMidTerm.CheckedChanged += new System.EventHandler(this.rbtnMidTerm_CheckedChanged);
            // 
            // rbtnShortTerm
            // 
            this.rbtnShortTerm.AutoSize = true;
            this.rbtnShortTerm.Location = new System.Drawing.Point(11, 37);
            this.rbtnShortTerm.Name = "rbtnShortTerm";
            this.rbtnShortTerm.Size = new System.Drawing.Size(194, 21);
            this.rbtnShortTerm.TabIndex = 0;
            this.rbtnShortTerm.TabStop = true;
            this.rbtnShortTerm.Text = "Short-term Evaluation (1 sec)";
            this.rbtnShortTerm.UseVisualStyleBackColor = true;
            this.rbtnShortTerm.CheckedChanged += new System.EventHandler(this.rbtnShortTerm_CheckedChanged);
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Controls.Add(this.label8);
            this.groupBox1.Controls.Add(this.lblDeltaX);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label7);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.lblAzimuth);
            this.groupBox1.Controls.Add(this.lblCentroidX);
            this.groupBox1.Controls.Add(this.lblDeltaY);
            this.groupBox1.Controls.Add(this.label6);
            this.groupBox1.Controls.Add(this.label4);
            this.groupBox1.Controls.Add(this.lblTotal);
            this.groupBox1.Controls.Add(this.lblCentroidY);
            this.groupBox1.Controls.Add(this.lblConsidered);
            this.groupBox1.Controls.Add(this.lblDelta);
            this.groupBox1.Controls.Add(this.label5);
            this.groupBox1.Font = new System.Drawing.Font("Segoe UI Variable Display Semib", 9.75F, System.Drawing.FontStyle.Bold);
            this.groupBox1.Location = new System.Drawing.Point(410, 46);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(459, 270);
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
            this.menuStrip1.Size = new System.Drawing.Size(904, 25);
            this.menuStrip1.TabIndex = 19;
            this.menuStrip1.Text = "menuStrip1";
            // 
            // fileToolStripMenuItem
            // 
            this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.openCSVToolStripMenuItem});
            this.fileToolStripMenuItem.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
            this.fileToolStripMenuItem.Size = new System.Drawing.Size(39, 21);
            this.fileToolStripMenuItem.Text = "File";
            // 
            // openCSVToolStripMenuItem
            // 
            this.openCSVToolStripMenuItem.Name = "openCSVToolStripMenuItem";
            this.openCSVToolStripMenuItem.Size = new System.Drawing.Size(157, 22);
            this.openCSVToolStripMenuItem.Text = "Open CSV File";
            this.openCSVToolStripMenuItem.Click += new System.EventHandler(this.openCSVFileToolStripMenuItem_Click);
            // 
            // openCSVDialog
            // 
            this.openCSVDialog.FileName = "openFileDialog1";
            // 
            // btnExport
            // 
            this.btnExport.Font = new System.Drawing.Font("Segoe Fluent Icons", 14.25F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnExport.Location = new System.Drawing.Point(22, 565);
            this.btnExport.Name = "btnExport";
            this.btnExport.Size = new System.Drawing.Size(244, 30);
            this.btnExport.TabIndex = 26;
            this.btnExport.Text = "";
            this.btnExport.UseVisualStyleBackColor = true;
            this.btnExport.Click += new System.EventHandler(this.btnExport_Click);
            // 
            // chkOpenBeforeSave
            // 
            this.chkOpenBeforeSave.AutoSize = true;
            this.chkOpenBeforeSave.Font = new System.Drawing.Font("Segoe UI Variable Display", 9.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.chkOpenBeforeSave.Location = new System.Drawing.Point(22, 601);
            this.chkOpenBeforeSave.Name = "chkOpenBeforeSave";
            this.chkOpenBeforeSave.Size = new System.Drawing.Size(190, 21);
            this.chkOpenBeforeSave.TabIndex = 27;
            this.chkOpenBeforeSave.Text = "Open and edit before saving";
            this.chkOpenBeforeSave.UseVisualStyleBackColor = true;
            // 
            // btnClear
            // 
            this.btnClear.Font = new System.Drawing.Font("Segoe Fluent Icons", 12.75F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.btnClear.Location = new System.Drawing.Point(271, 565);
            this.btnClear.Margin = new System.Windows.Forms.Padding(2);
            this.btnClear.Name = "btnClear";
            this.btnClear.Size = new System.Drawing.Size(103, 30);
            this.btnClear.TabIndex = 28;
            this.btnClear.Text = "";
            this.btnClear.UseVisualStyleBackColor = true;
            this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
            // 
            // FrmMain
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(96F, 96F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Dpi;
            this.BackColor = System.Drawing.Color.White;
            this.ClientSize = new System.Drawing.Size(904, 645);
            this.Controls.Add(this.btnClear);
            this.Controls.Add(this.chkOpenBeforeSave);
            this.Controls.Add(this.btnExport);
            this.Controls.Add(this.gbSettings);
            this.Controls.Add(this.gbUnits);
            this.Controls.Add(this.lvPoints);
            this.Controls.Add(this.groupBox1);
            this.Controls.Add(this.menuStrip1);
            this.ForeColor = System.Drawing.Color.Black;
            this.FormBorderStyle = System.Windows.Forms.FormBorderStyle.FixedSingle;
            this.MainMenuStrip = this.menuStrip1;
            this.MaximizeBox = false;
            this.Name = "FrmMain";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Text = "Beam Position Stability";
            this.Load += new System.EventHandler(this.Form1_Load);
            this.gbUnits.ResumeLayout(false);
            this.gbUnits.PerformLayout();
            this.gbSettings.ResumeLayout(false);
            this.gbSettings.PerformLayout();
            this.groupBox1.ResumeLayout(false);
            this.menuStrip1.ResumeLayout(false);
            this.menuStrip1.PerformLayout();
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
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.Label label8;
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
    }
}

