#region Using directives
using FTOptix.Alarm;
using FTOptix.Core;
using FTOptix.CoreBase;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
#endregion

public class ImportExportAllAlarms : BaseNetLogic {
    //private NodeId alarmType = null;

    [ExportMethod]
    public void ImportAlarms() {
        List<string> commonProperty = new List<string>() { "Enabled", "AutoAcknowledge", "AutoConfirm", "Severity", "Message", "HighHighLimit", "HighLimit", "LowLowLimit", "LowLimit", "LastEvent", "InputValue", "InputValueArrayIndex", "NormalStateValue" };
        var folderPath = GetCSVFilePath();
        if (string.IsNullOrEmpty(folderPath)) {
            Log.Error("AlarmImportAndExport", "No CSV file chosen, please fill the CSVPath variable");
            return;
        }

        char fieldDelimiter = (char)GetFieldDelimiter();
        if (fieldDelimiter == '\0')
            return;

        bool wrapFields = GetWrapFields();

        //if (!File.Exists(csvPath))
        //{
        //    Log.Error("AlarmImporter", "The file " + csvPath + " does not exist");
        //    return;
        //}

        foreach (string file in Directory.EnumerateFiles(folderPath, "*.csv")) {
            try {
                using (var reader = new CSVFileReader(file) { FieldDelimiter = fieldDelimiter, WrapFields = wrapFields })//new StreamReader(file))
                {
                    var csvUaObjects = new List<CsvUaObject>();

                    var headerColumns = reader.ReadLine();//.Split(fieldDelimiter).Where((v, i) => i > (CsvUaObject.GetCsvFixedHeaderColumns().Length - 1)).ToList();

                    while (!reader.EndOfFile())//EndOfStream)
                    {
                        //var obj = GetDataFromCsvRow(reader.ReadLine(), headerColumns, fieldDelimiter);
                        var obj = GetDataFromCsvRow(reader.ReadLine(), headerColumns);

                        if (obj == null) { continue; }
                        csvUaObjects.Add(obj);
                    }

                    if (csvUaObjects.Count == 0) {
                        //Log.Error(MethodBase.GetCurrentMethod().Name, file + ":" + $"No valid objects to import.");
                        continue;
                    }

                    var objectTypesIntoFile = csvUaObjects.Select(o => o.TypeBrowsePath).Distinct().ToList();

                    if (objectTypesIntoFile.Count > 1) {
                        Log.Error(MethodBase.GetCurrentMethod().Name, $"Csv file contains data of more than one object type: {string.Join(",", objectTypesIntoFile)}. Aborting.");
                        return;
                    }

                    var csvObjectsCommonType = objectTypesIntoFile.FirstOrDefault().Split('/').LastOrDefault();

                    var objectType = Project.Current.Find(csvObjectsCommonType);

                    //if (objectType == null)
                    //{
                    //    Log.Error(MethodBase.GetCurrentMethod().Name, $"Object Type {csvObjectsCommonType} does not exist into the Uniqo project");
                    //    return;
                    //}

                    foreach (var item in csvUaObjects) {
                        CreateFoldersTreeFromPath(item.BrowsePath);
                        Project.Current.Get(item.BrowsePath).Children.Remove(item.Name);
                        IUAObject myNewAlarm = null;
                        if (objectType == null && csvObjectsCommonType.Contains("Controller")) {
                            var s = csvObjectsCommonType.Replace("\"", "");
                            switch (s) {
                                case "OffNormalAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<DigitalAlarm>(item.Name);
                                    break;
                                case "ExclusiveLevelAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<ExclusiveLevelAlarmController>(item.Name);
                                    break;
                                case "NonExclusiveLevelAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<NonExclusiveLevelAlarmController>(item.Name);
                                    break;
                                case "ExclusiveDeviationAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<ExclusiveDeviationAlarmController>(item.Name);
                                    break;
                                case "NonExclusiveDeviationAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<NonExclusiveDeviationAlarmController>(item.Name);
                                    break;
                                case "ExclusiveRateOfChangeAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<ExclusiveRateOfChangeAlarmController>(item.Name);
                                    break;
                                case "NonExclusiveRateOfChangeAlarmController":
                                    myNewAlarm = InformationModel.MakeObject<NonExclusiveRateOfChangeAlarmController>(item.Name);
                                    break;
                                default:
                                    break;
                            }
                        } else if (objectType != null)
                            myNewAlarm = InformationModel.MakeObject(item.Name, objectType.NodeId);
                        else {
                            Log.Error(MethodBase.GetCurrentMethod().Name, $"Object Type {csvObjectsCommonType} does not exist into the Uniqo project");
                            continue;
                        }

                        SetInputValueProperty((AlarmController)myNewAlarm, item);
                        TrySetBooleanOptionalProperty((AlarmController)myNewAlarm, "Enabled", item.Variables.SingleOrDefault(v => v.Key == "Enabled").Value);
                        TrySetBooleanOptionalProperty((AlarmController)myNewAlarm, "AutoAcknowledge", item.Variables.SingleOrDefault(v => v.Key == "AutoAcknowledge").Value);
                        TrySetBooleanOptionalProperty((AlarmController)myNewAlarm, "AutoConfirm", item.Variables.SingleOrDefault(v => v.Key == "AutoConfirm").Value);
                        
                        SetSeverityProperty((AlarmController)myNewAlarm, item);
                        
                        TrySetDoubleOptionalProperty((AlarmController)myNewAlarm, "NormalStateValue", item.Variables.SingleOrDefault(v => v.Key == "NormalStateValue").Value);

                        TrySetDoubleOptionalProperty((AlarmController)myNewAlarm, "HighHighLimit", item.Variables.SingleOrDefault(v => v.Key == "HighHighLimit").Value);
                        TrySetDoubleOptionalProperty((AlarmController)myNewAlarm, "HighLimit", item.Variables.SingleOrDefault(v => v.Key == "HighLimit").Value);
                        TrySetDoubleOptionalProperty((AlarmController)myNewAlarm, "LowLimit", item.Variables.SingleOrDefault(v => v.Key == "LowLimit").Value);
                        TrySetDoubleOptionalProperty((AlarmController)myNewAlarm, "LowLowLimit", item.Variables.SingleOrDefault(v => v.Key == "LowLowLimit").Value);

                        var message = item.Variables.SingleOrDefault(v => v.Key == "Message").Value;

                        // Interpret the message field read by the current alarm as TextID if MessageAsTranslationKey is set to true and
                        // perform a lookup in the translation table
                        if (GetMessageAsTranslationKey()) {
                            var localizedMessage = new LocalizedText(message);
                            if (!InformationModel.LookupTranslation(localizedMessage).HasTranslation) {
                                Log.Warning("AlarmImportAndExport", $"Alarm {myNewAlarm.BrowseName} Message with translation key \"{message}\" was not found (MessageAsTranslationKey = true)");
                                return;
                            }

                            ((AlarmController)myNewAlarm).LocalizedMessage = localizedMessage;
                        } else if (!string.IsNullOrEmpty(message)) {
                            ((AlarmController)myNewAlarm).Message = message;
                        }

                        foreach (var property in myNewAlarm.Children) {
                            if (commonProperty.Contains(property.BrowseName))
                                continue;
                            myNewAlarm.GetVariable(property.BrowseName).Value = item.Variables.SingleOrDefault(v => v.Key == property.BrowseName).Value;
                        }
                        Project.Current.Get(item.BrowsePath).Children.Add(myNewAlarm);
                    }
                }
                Log.Info("AlarmImporter", "Alarms successfully imported from " + file);
            } catch (Exception ex) {
                Log.Error("AlarmImporter", "Unable to import alarms from " + file + ": " + ex.ToString());
            }
        }


    }

    private bool ConvertStringToBool(string value) {
        return value == "1" || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    private void TrySetBooleanOptionalProperty(AlarmController alarm, string propertyName, string propertyValue) {
        if (propertyValue == "" || propertyValue == null)
            return;

        alarm.SetOptionalVariableValue(propertyName, ConvertStringToBool(propertyValue));
    }
    private void SetInputValueProperty(AlarmController alarm, CsvUaObject alarmFields) {
        var value = alarmFields.Variables.SingleOrDefault(v => v.Key == "InputValue").Value;

        if (value == "" || value == null)
            return;

        var inputVariable = Project.Current.GetVariable(alarmFields.Variables.SingleOrDefault(v => v.Key == "InputValue").Value);
        if (inputVariable == null)
            throw new Exception("input variable not found");

        alarm.InputValueVariable.ResetDynamicLink();

        if (uint.TryParse(alarmFields.Variables.SingleOrDefault(v => v.Key == "InputValueArrayIndex").Value, out uint index))
            alarm.InputValueVariable.SetDynamicLink(inputVariable, index);
        else
            alarm.InputValueVariable.SetDynamicLink(inputVariable);
    }

    private void SetSeverityProperty(AlarmController alarm, CsvUaObject alarmFields) {
        var value = alarmFields.Variables.SingleOrDefault(v => v.Key == "Severity").Value;

        if (value == "" || value == null){
            return;}

        var severityVariable = Project.Current.GetVariable(alarmFields.Variables.SingleOrDefault(v => v.Key == "Severity").Value);
        if (severityVariable == null){
            TrySetUshortOptionalProperty(alarm, "Severity", alarmFields.Variables.SingleOrDefault(v => v.Key == "Severity").Value);
            return;
        }

        alarm.SeverityVariable.ResetDynamicLink();

        if (uint.TryParse(alarmFields.Variables.SingleOrDefault(v => v.Key == "InputValueArrayIndex").Value, out uint index))
            alarm.SeverityVariable.SetDynamicLink(severityVariable, index);
        else
            alarm.SeverityVariable.SetDynamicLink(severityVariable);
    }

    private void TrySetUshortOptionalProperty(AlarmController alarm, string propertyName, string propertyValue) {
        if (propertyValue == "" || propertyValue == null)
            return;

        if (!ushort.TryParse(propertyValue, out ushort value))
            throw new Exception("Parameter " + propertyName + " is not a valid ushort");

        alarm.SetOptionalVariableValue(propertyName, value);
    }

    private void TrySetDoubleOptionalProperty(AlarmController alarm, string propertyName, string propertyValue) {
        if (propertyValue == "" || propertyValue == null)
            return;

        if (!double.TryParse(propertyValue, out double value))
            throw new Exception("Parameter " + propertyName + "is not a valid double");

        alarm.SetOptionalVariableValue(propertyName, value);
    }

    private List<IUAObjectType> GetAlarmTypeList() {
        var alarms = new List<IUAObjectType>();
        var projectNamespaceIndex = LogicObject.NodeId.NamespaceIndex;
        // Insert code to be executed by the method
        var alamrControllerType = InformationModel.Get(FTOptix.Alarm.ObjectTypes.AlarmController);
        var allControllerTypes = new List<IUAObjectType>();
        CollectRecursive((IUAObjectType)alamrControllerType, allControllerTypes);
        var concreteTypes = allControllerTypes.FindAll(type => !type.IsAbstract);
        //Log.Info("ALL ALARM CONTROLLER TYPE ARE:");
        foreach (var e in concreteTypes)
            alarms.Add(e);
        var userDefinedTypes = concreteTypes.FindAll(type => type.NodeId.NamespaceIndex == projectNamespaceIndex);
        foreach (var e in userDefinedTypes) {
            alarms.Add(e);
        }
        return alarms;
    }


    [ExportMethod]
    public void ExportAlarms() {
        var csvPath = GetCSVFilePath();
        if (string.IsNullOrEmpty(csvPath)) {
            Log.Error("AlarmImportAndExport", "No CSV file chosen, please fill the CSVPath variable");
            return;
        }

        char? fieldDelimiter = GetFieldDelimiter();
        if (fieldDelimiter == null || fieldDelimiter == '\0')
            return;

        bool wrapFields = GetWrapFields();

        List<IUAObjectType> typesAlarm = GetAlarmTypeList();

        foreach (var alarmCustomType in typesAlarm) {
            string pathalarmType = GetBrowsePathFromIuaNode(InformationModel.Get(alarmCustomType.NodeId));
            List<string> propertiesFields = new List<string>();
            List<string> valuesFields = new List<string>();
            propertiesFields.Add("Name");
            propertiesFields.Add("Type");
            propertiesFields.Add("Path");
            CheckAlarmProperties(alarmCustomType.NodeId, propertiesFields);

            try {
                using (var csvWriter = new CSVFileWriter(csvPath + "/" + alarmCustomType.BrowseName + ".csv") { FieldDelimiter = fieldDelimiter.Value, WrapFields = wrapFields }) {
                    csvWriter.WriteLine(propertiesFields.ToArray());

                    foreach (var alarm in GetAlarmList(alarmCustomType.NodeId)) {
                        var alarmFields = CollectAlarmConfiguration(alarm);

                        valuesFields = new List<string>();
                        valuesFields.Add(alarm.BrowseName);
                        valuesFields.Add(pathalarmType);
                        valuesFields.Add(GetBrowsePathWithoutNodeName(alarm));

                        foreach (var item in propertiesFields) {
                            if (item == "Name" || item == "Type" || item == "Path" || item == "InputValueArrayIndex")
                                continue;
                            if (item == "InputValue") {
                                List<string> inputFields = new List<string>();
                                ExportAlarmInputVariable((AlarmController)alarm, inputFields);
                                foreach (var inputField in inputFields) {
                                    valuesFields.Add(inputField);
                                }
                            } else if (item == "Message") {
                                if (GetMessageAsTranslationKey()) {
                                    // When MessageAsTranslationKey is set to true, we need to export the TextID of Message (not the Message Text)
                                    var localizedTextMessage = ((AlarmController)alarm).LocalizedMessage;
                                    if (localizedTextMessage != null && localizedTextMessage.HasTextId)
                                        valuesFields.Add(localizedTextMessage.TextId);
                                    else {
                                        Log.Warning("AlarmImportAndExport", $"Message of alarm {alarm.BrowseName} has no translation key. Message of this alarm will not exported (MessageAsTranslationKey = true)");
                                        valuesFields.Add("");
                                    }
                                } else {
                                    // When MessageAsTranslationKey is set to false, we need to export the content of Message
                                    if (alarm.GetVariable("Message") != null)
                                        valuesFields.Add(((AlarmController)alarm).Message);
                                    else
                                        valuesFields.Add("");
                                }
                            } else if (item == "Severity") {
                                List<string> inputFields = new List<string>();
                                ExportAlarmSeverityVariable((AlarmController)alarm, inputFields);
                                foreach (var inputField in inputFields) {
                                    valuesFields.Add(inputField);
                                }
                            } else {
                                if (((AlarmController)alarm).GetVariable(item) != null)
                                    valuesFields.Add(((AlarmController)alarm).GetVariable(item).Value);
                                else
                                    valuesFields.Add("");
                            }

                        }
                        csvWriter.WriteLine(valuesFields.ToArray());
                    }
                }
            } catch (Exception ex) {
                Log.Error("AlarmExporter", "Unable to export alarms: " + ex);
            }
        }
        Log.Info("AlarmExporter", "Alarms successfully exported to " + csvPath);
        return;
    }

    private string GetBrowsePathWithoutNodeName(IUANode uaObj) {
        var browsePath = GetBrowsePathFromIuaNode(uaObj);
        return browsePath.Contains("/") ? browsePath.Substring(0, browsePath.LastIndexOf("/", StringComparison.Ordinal)) : browsePath;
    }

    private string GetBrowsePathFromIuaNode(IUANode uaNode) => ClearPathFromProjectInfo(Log.Node(uaNode));
    private string ClearPathFromProjectInfo(string path) {
        var projectName = Project.Current.BrowseName + "/";
        var occurrence = path.IndexOf(projectName);
        if (occurrence == -1) { return path; }

        path = path.Substring(occurrence + projectName.Length);
        return path;
    }

    private void ExportAlarmInputVariable(AlarmController alarm, List<string> alarmFields) {
        var inputPath = (DynamicLink)alarm.InputValueVariable.Children.GetVariable("DynamicLink");
        if (inputPath == null) {
            alarmFields.Add("");
            alarmFields.Add("");
        } else {
            var inputValue = LogicObject.Context.ResolvePath(alarm.InputValueVariable, inputPath.Value).ResolvedNode;
            var pathToValue = MakeBrowsePath(inputValue);
            alarmFields.Add(pathToValue);

            var sourceArrayIndexVariable = inputPath.GetVariable("SourceArrayIndex");
            if (sourceArrayIndexVariable != null) {
                var value = sourceArrayIndexVariable.Value.Value;
                if (value.GetType().IsArray) {
                    uint[] index = sourceArrayIndexVariable.Value;
                    alarmFields.Add(index[0].ToString());
                } else {
                    uint index = sourceArrayIndexVariable.Value;
                    alarmFields.Add(index.ToString());
                }

            } else
                alarmFields.Add("");
        }
    }

    private void ExportAlarmSeverityVariable(AlarmController alarm, List<string> alarmFields) {
        var inputPath = (DynamicLink)alarm.SeverityVariable.Children.GetVariable("DynamicLink");
        if (inputPath == null) {
            if (((AlarmController)alarm).GetVariable("Severity") != null)
                alarmFields.Add(((AlarmController)alarm).GetVariable("Severity").Value);
            else
                alarmFields.Add("");
        } else {
            var inputValue = LogicObject.Context.ResolvePath(alarm.SeverityVariable, inputPath.Value).ResolvedNode;
            var pathToValue = MakeBrowsePath(inputValue);
            alarmFields.Add(pathToValue);

            var sourceArrayIndexVariable = inputPath.GetVariable("SourceArrayIndex");
            if (sourceArrayIndexVariable != null) {
                var value = sourceArrayIndexVariable.Value.Value;
                if (value.GetType().IsArray) {
                    uint[] index = sourceArrayIndexVariable.Value;
                    alarmFields.Add(index[0].ToString());
                } else {
                    uint index = sourceArrayIndexVariable.Value;
                    alarmFields.Add(index.ToString());
                }

            } else
                alarmFields.Add("");
        }
    }

    private string MakeBrowsePath(IUANode node) {
        string path = node.BrowseName;
        var current = node.Owner;

        while (current != Project.Current) {
            path = current.BrowseName + "/" + path;
            current = current.Owner;
        }
        return path;
    }
    private List<string> CollectAlarmConfiguration(IUAObject alarm) {
        var alarmFields = new List<string>();

        foreach (var item in alarm.Children) {
            alarmFields.Add(item.BrowseName);
        }

        return alarmFields;
    }

    private List<IUAObject> GetAlarmList(NodeId alarmTypeNodeId) {
        var alarms = new List<IUAObject>();
        var typedAlarms = GetAlarmsByType(alarmTypeNodeId);
        foreach (var typedAlarm in typedAlarms)
            alarms.Add(typedAlarm);

        return alarms;
    }

    private IReadOnlyList<IUAObject> GetAlarmsByType(NodeId type) {
        var alarmType = LogicObject.Context.GetObjectType(type);
        var alarms = alarmType.InverseRefs.GetObjects(OpcUa.ReferenceTypes.HasTypeDefinition, false);
        return alarms;
    }

    private bool GetMessageAsTranslationKey() {
        var messageAsTranslationKeyVariable = LogicObject.GetVariable("MessageAsTranslationKey");
        return messageAsTranslationKeyVariable == null ? false : (bool)messageAsTranslationKeyVariable.Value;
    }

    private string GetCSVFilePath() {
        var csvPathVariable = LogicObject.Children.Get<IUAVariable>("CSVPath");
        if (csvPathVariable == null) {
            Log.Error("AlarmImportAndExport", "CSVPath variable not found");
            return "";
        }

        return new ResourceUri(csvPathVariable.Value).Uri;
    }

    private char? GetFieldDelimiter() {
        var separatorVariable = LogicObject.GetVariable("CharacterSeparator");
        if (separatorVariable == null) {
            Log.Error("AlarmImportAndExport", "CharacterSeparator variable not found");
            return null;
        }

        string separator = separatorVariable.Value;

        if (separator.Length != 1 || separator == String.Empty) {
            Log.Error("AlarmImportAndExport", "Wrong CharacterSeparator configuration. Please insert a char");
            return null;
        }

        if (char.TryParse(separator, out char result))
            return result;

        return null;
    }

    private bool GetWrapFields() {
        var wrapFieldsVariable = LogicObject.GetVariable("WrapFields");
        if (wrapFieldsVariable == null) {
            Log.Error("AlarmImportAndExport", "WrapFields variable not found");
            return false;
        }

        return wrapFieldsVariable.Value;
    }

    private NodeId GetAlarmType() {
        var alarmType = LogicObject.GetVariable("AlarmType");
        if (alarmType.Value.Value == null) {
            Log.Error("AlarmImportAndExport", "AlarmType variable not found");
            return null;
        }
        return InformationModel.Get(alarmType.Value).NodeId;
    }

    private static bool CreateFoldersTreeFromPath(string path) {

        if (string.IsNullOrEmpty(path)) { return true; }
        var segments = path.Split('/').ToList();
        string updatedSegment = "";
        string segmentsAccumulator = "";

        try {
            foreach (var s in segments) {
                if (segmentsAccumulator == "")
                    updatedSegment = s;
                else
                    updatedSegment = updatedSegment + "/" + s;
                var folder = InformationModel.MakeObject<Folder>(s);
                var folderAlreadyExists = Project.Current.GetObject(updatedSegment) != null;
                if (!folderAlreadyExists) {
                    if (segmentsAccumulator == "")
                        Project.Current.Add(folder);
                    else
                        Project.Current.GetObject(segmentsAccumulator).Children.Add(folder);
                }
                segmentsAccumulator = updatedSegment;
            }
        } catch (Exception e) {
            Log.Error(MethodBase.GetCurrentMethod().Name, $"Cannot create folder, error {e.Message}");
            return false;
        }
        return true;
    }

    private CsvUaObject GetDataFromCsvRow(List<string> line, List<string> header) {
        var csvUaObject = new CsvUaObject {
            Name = line[0],
            TypeBrowsePath = line[1],
            BrowsePath = line[2]
        };

        if (!csvUaObject.IsValid()) {
            Log.Error(MethodBase.GetCurrentMethod().Name, $"Invalid object with name {csvUaObject.Name}. Please check its properties.");
            return null;
        }

        for (var i = 3; i < header.Count; i++) {
            csvUaObject.Variables.Add(header[i], line[i]);
        }

        return csvUaObject;
    }

    private class CsvUaObject {
        private const string CSV_NAME_COLUMN = "Name";
        private const string CSV_TYPE_COLUMN = "Type";
        private const string CSV_PATH_COLUMN = "Path";
        public const string CSV_INPUT_VALUE_PATH_COLUMN = "InputValuePath";
        public const string CSV_INPUT_VALUE_COLUMN = "InputValue";
        private static readonly string[] CSV_VARIABLES_STARTING_HEADER = new string[] { CSV_NAME_COLUMN, CSV_TYPE_COLUMN, CSV_PATH_COLUMN };

        public string Name { get; set; }
        public string TypeBrowsePath { get; set; }
        public string BrowsePath { get; set; }
        public Dictionary<string, string> Variables { get; set; } = new Dictionary<string, string>();

        public bool IsValid() {
            return !string.IsNullOrWhiteSpace(TypeBrowsePath)
                    && !string.IsNullOrWhiteSpace(Name)
                        && !string.IsNullOrWhiteSpace(BrowsePath);
        }

        public static string[] GetCsvFixedHeaderColumns() => CSV_VARIABLES_STARTING_HEADER;

        internal static void WriteToCsv(List<CsvUaObject> csvUaObjects, List<string> csvColumnsNames, CSVFileWriter csvWriter) {
            foreach (var o in csvUaObjects) {
                if (!o.IsValid()) { Log.Error(MethodBase.GetCurrentMethod().Name, $"Cannot export object {o.Name}: not Valid"); }
                var csvRow = new List<string>() { o.Name, o.TypeBrowsePath, o.BrowsePath };

                foreach (var column in csvColumnsNames) {
                    var objVariable = o.Variables.SingleOrDefault(v => v.Key == column);
                    if (objVariable.Equals(new KeyValuePair<string, string>())) {
                        csvRow.Add(string.Empty);
                        continue;
                    }
                    csvRow.Add(objVariable.Value);
                }

                try {
                    csvWriter.WriteLine(csvRow.ToArray());
                } catch (Exception e) {
                    Log.Error(MethodBase.GetCurrentMethod().Name, $"Cannot export object {o.Name}, error: {e.Message}");
                }
            }
        }
    }
    private class CSVFileReader : IDisposable {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public bool IgnoreMalformedLines { get; set; } = false;

        public CSVFileReader(string filePath, System.Text.Encoding encoding) {
            streamReader = new StreamReader(filePath, encoding);
        }

        public CSVFileReader(string filePath) {
            streamReader = new StreamReader(filePath, System.Text.Encoding.UTF8);
        }

        public CSVFileReader(StreamReader streamReader) {
            this.streamReader = streamReader;
        }

        public bool EndOfFile() {
            return streamReader.EndOfStream;
        }

        public List<string> ReadLine() {
            if (EndOfFile())
                return null;

            var line = streamReader.ReadLine();

            var result = WrapFields ? ParseLineWrappingFields(line) : ParseLineWithoutWrappingFields(line);

            currentLineNumber++;
            return result;

        }

        public List<List<string>> ReadAll() {
            var result = new List<List<string>>();
            while (!EndOfFile())
                result.Add(ReadLine());

            return result;
        }

        private List<string> ParseLineWithoutWrappingFields(string line) {
            if (string.IsNullOrEmpty(line) && !IgnoreMalformedLines)
                throw new FormatException($"Error processing line {currentLineNumber}. Line cannot be empty");

            return line.Split(FieldDelimiter).ToList();
        }

        private List<string> ParseLineWrappingFields(string line) {
            var fields = new List<string>();
            var buffer = new StringBuilder("");
            var fieldParsing = false;

            int i = 0;
            while (i < line.Length) {
                if (!fieldParsing) {
                    if (IsWhiteSpace(line, i)) {
                        ++i;
                        continue;
                    }

                    // Line and column numbers must be 1-based for messages to user
                    var lineErrorMessage = $"Error processing line {currentLineNumber}";
                    if (i == 0) {
                        // A line must begin with the quotation mark
                        if (!IsQuoteChar(line, i)) {
                            if (IgnoreMalformedLines)
                                return null;
                            else
                                throw new FormatException($"{lineErrorMessage}. Expected quotation marks at column {i + 1}");
                        }

                        fieldParsing = true;
                    } else {
                        if (IsQuoteChar(line, i))
                            fieldParsing = true;
                        else if (!IsFieldDelimiter(line, i)) {
                            if (IgnoreMalformedLines)
                                return null;
                            else
                                throw new FormatException($"{lineErrorMessage}. Wrong field delimiter at column {i + 1}");
                        }
                    }

                    ++i;
                } else {
                    if (IsEscapedQuoteChar(line, i)) {
                        i += 2;
                        buffer.Append(QuoteChar);
                    } else if (IsQuoteChar(line, i)) {
                        fields.Add(buffer.ToString());
                        buffer.Clear();
                        fieldParsing = false;
                        ++i;
                    } else {
                        buffer.Append(line[i]);
                        ++i;
                    }
                }
            }

            return fields;
        }

        private bool IsEscapedQuoteChar(string line, int i) {
            return line[i] == QuoteChar && i != line.Length - 1 && line[i + 1] == QuoteChar;
        }

        private bool IsQuoteChar(string line, int i) {
            return line[i] == QuoteChar;
        }

        private bool IsFieldDelimiter(string line, int i) {
            return line[i] == FieldDelimiter;
        }

        private bool IsWhiteSpace(string line, int i) {
            return Char.IsWhiteSpace(line[i]);
        }

        private StreamReader streamReader;
        private int currentLineNumber = 1;

        #region IDisposable support
        private bool disposed = false;
        protected virtual void Dispose(bool disposing) {
            if (disposed)
                return;

            if (disposing)
                streamReader.Dispose();

            disposed = true;
        }

        public void Dispose() {
            Dispose(true);
        }
        #endregion
    }

    private class CSVFileWriter : IDisposable {
        public char FieldDelimiter { get; set; } = ',';

        public char QuoteChar { get; set; } = '"';

        public bool WrapFields { get; set; } = false;

        public CSVFileWriter(string filePath) {
            streamWriter = new StreamWriter(filePath, false, System.Text.Encoding.UTF8);
        }

        public CSVFileWriter(string filePath, System.Text.Encoding encoding) {
            streamWriter = new StreamWriter(filePath, false, encoding);
        }

        public CSVFileWriter(StreamWriter streamWriter) {
            this.streamWriter = streamWriter;
        }

        public void WriteLine(string[] fields) {
            var stringBuilder = new StringBuilder();

            for (var i = 0; i < fields.Length; ++i) {
                if (WrapFields)
                    stringBuilder.AppendFormat("{0}{1}{0}", QuoteChar, EscapeField(fields[i]));
                else
                    stringBuilder.AppendFormat("{0}", fields[i]);

                if (i != fields.Length - 1)
                    stringBuilder.Append(FieldDelimiter);
            }

            streamWriter.WriteLine(stringBuilder.ToString());
            streamWriter.Flush();
        }

        private string EscapeField(string field) {
            var quoteCharString = QuoteChar.ToString();
            return field.Replace(quoteCharString, quoteCharString + quoteCharString);
        }

        private StreamWriter streamWriter;

        #region IDisposable Support
        private bool disposed = false;
        protected virtual void Dispose(bool disposing) {
            if (disposed)
                return;

            if (disposing)
                streamWriter.Dispose();

            disposed = true;
        }

        public void Dispose() {
            Dispose(true);
        }

        #endregion
    }

    private void CheckAlarmProperties(NodeId alarmType, List<string> propertyList) {
        List<string> commonProperty = new List<string>() { "Enabled", "AutoAcknowledge", "AutoConfirm", "Severity", "Message", "HighHighLimit", "HighLimit", "LowLowLimit", "LowLimit", "InputValue", "InputValueArrayIndex", "NormalStateValue" };

        IUANode myAlarm = InformationModel.Get(alarmType);
        IUAObjectType myAlarmSuperType = ((UAObjectType)myAlarm).SuperType;

        while (myAlarmSuperType != null) {
            if (myAlarmSuperType.BrowseName == "AlarmController" || myAlarmSuperType.BrowseName == "LimitAlarmController") {
                foreach (var item in myAlarmSuperType.Children) {
                    if (commonProperty.Contains(item.BrowseName)) {
                        propertyList.Add(item.BrowseName);
                        if (item.BrowseName == "InputValue")
                            propertyList.Add("InputValueArrayIndex");
                    }


                }
            }
            myAlarmSuperType = myAlarmSuperType.SuperType;
        }

        foreach (var item in InformationModel.Get(alarmType).Children) {
            if (propertyList.Contains(item.BrowseName) || item.BrowseName == "LastEvent")
                continue;
            propertyList.Add(item.BrowseName);
        }

    }

    void CollectRecursive(IUAObjectType parentType, List<IUAObjectType> allControllerTypes) {
        allControllerTypes.Add(parentType);
        foreach (var childType in parentType.Refs.GetObjectTypes(OpcUa.ReferenceTypes.HasSubtype, false))
            CollectRecursive(childType, allControllerTypes);
    }

}
