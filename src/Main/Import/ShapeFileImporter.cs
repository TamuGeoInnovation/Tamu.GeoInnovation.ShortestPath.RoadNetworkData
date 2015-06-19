using System;
using System.ComponentModel;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
	public enum DataProvider { TeleAtlas, Navteq, Empty };

	public abstract class ShapeFileImporter
	{
		protected double totalFilesBytes;
		protected double currentFilePercent;
		protected double completedPercent;

		public ShapeFileImporter()
		{
			totalFilesBytes = 0;
			currentFilePercent = 0;
			completedPercent = 0;
		}

		protected static string GetRemainingTime(DateTime start, double percent)
		{
			string sentense = "";
			double passedMin = (DateTime.Now - start).TotalMinutes;
			if ((percent < 3) && (passedMin < 4)) sentense = "(estimating remaining time ...)";
			else
			{
				int remainMin = Convert.ToInt32(passedMin / percent * (100 - percent));
				if (remainMin < 1) sentense = "Almost Done";
				else
				{
					if (remainMin < 60)
						sentense = "Remaining: " + remainMin + " min(s)";
					else if (remainMin < 1440)
						sentense = "Remaining: " + (remainMin / 60) + " hour(s) and " + (((remainMin % 60) / 5) * 5) + " min(s)";
					else
						sentense = "Remaining: " + (remainMin / 1440) + " day(s) and " + ((remainMin % 1440) / 60) + " hour(s)";
				}
			}
			return sentense;
		}

		public abstract ShapeFileImporterOutput SaveToSQL(BackgroundWorker worker, RoadNetworkDBManagerInput e);

		public abstract ShapeFileImporterOutput SaveToDisk(BackgroundWorker worker, RoadNetworkDBManagerInput e);

	}
}