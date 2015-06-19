namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
  public class ShapeFileImporterOutput
  {
    public int ProcessedFilesCount;
    public int ProcessedInsertCount;
    public bool Cancelled;
    public int ErrorCount;

    public ShapeFileImporterOutput()
    {
      ProcessedFilesCount = 0;
      ProcessedInsertCount = 0;
      ErrorCount = 0;
      Cancelled = false;
    }
  }
}
