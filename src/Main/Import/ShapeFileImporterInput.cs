using System;
using System.Xml.Serialization;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
  [Serializable]
  public class ShapeFileImporterInput
  {
    public DataProvider MyDataProvider;
    public string RootDirectory;
    public bool SetAsPrimary;
    [XmlIgnore]
    public int DataYear;
    [XmlIgnore]
    public int DataMonth;
    [XmlIgnore]
    public string DataDescription;
    [XmlIgnore]
    public string RoadNetworkDatabaseName; // This will be used as table name for this newly imported data
  }
}