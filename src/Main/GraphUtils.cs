using System;
using System.Data;
using System.Data.SqlClient;
using USC.GISResearchLab.ShortestPath.GraphStructure;

namespace USC.GISResearchLab.ShortestPath.RoadNetworkData
{
    public class GraphUtils
    {
        public static Graph BuildGraphFromSQL(string conStr)
        {
            // ---------- Start creating sqldatareader

            string tableName = string.Empty;
            string tableDscr = string.Empty;
            var tbl = new DataTable();
            var selectPrimaryTableStr = "Select associatedTableName,recordCount,description from AvailableRoadNetworkData where (isPrimary = 1) and (recordCount > 0) and (isErr = 0)";
            var selectStreets = "Select [fr_spd_lim], [to_spd_lim], [len], [dir_travel], [speed_cat], [link_id], [fromlong], [fromlat], [tolong], [tolat], [private] from ";
            var adpt = new SqlDataAdapter(selectPrimaryTableStr, conStr);
            var con = new SqlConnection(conStr);

            adpt.Fill(tbl);
            tableName = (string)(tbl.Rows[0].ItemArray[0]);
            tableDscr = (string)(tbl.Rows[0].ItemArray[2]);
            int tableCap = Convert.ToInt32((int)(tbl.Rows[0].ItemArray[1]) / 1.32);
            selectStreets += tableName + "_Streets where ([ar_auto] = 'Y') and ([speed_cat] <> ' ') and (ferry_type = 'H')";
            adpt.Dispose();
            adpt = null;
            tbl = null;

            System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": Build Graph Query");

            con.Open();
            var cmd = new SqlCommand(selectStreets, con);
            cmd.CommandTimeout = 0;
            var streetsReader = cmd.ExecuteReader();

            // ---------- Building graph

            Graph g = new Graph(tableName, tableDscr, tableCap);

            // set spatial switch here
            g.IsSpatialEnabled = true;
            GraphNode startNode = null, endNode = null;
            double ft_speed, tf_speed, seg_len;
            string tavel_dir = string.Empty;
            int lineID, i = 0, j = 0;
            Int16 speed = 0;
            long tempUID = 0;
            bool privateRoad = false;

            System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": Build Graph starts");

            while (streetsReader.Read())
            {
                i++;
                #region Navteq

                // get and parse data from database         
                // costs
                ft_speed = streetsReader.GetInt32(0);
                tf_speed = streetsReader.GetInt32(1);
                seg_len = Convert.ToDouble(streetsReader.GetDecimal(2)); // miles

                // one-way & speed limitation
                tavel_dir = streetsReader.GetString(3);
                speed = GetSpeedFromSpeedCat(streetsReader.GetString(4)); // MPH

                // dynamap ID
                lineID = streetsReader.GetInt32(5);

                // create start node and end node from the database data fields
                // start node
                startNode = null; endNode = null;
                startNode = new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(6)), Convert.ToDouble(streetsReader.GetDecimal(7)));
                endNode = new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(8)), Convert.ToDouble(streetsReader.GetDecimal(9)));
                privateRoad = streetsReader.GetString(10) != "N";

                tempUID = startNode.UID;
                if (g.Contains(tempUID))
                {
                    startNode = null;
                    startNode = g.GetNode(tempUID);
                }
                else
                {
                    g.InsertNode(tempUID, startNode);
                }

                tempUID = endNode.UID;
                if (g.Contains(tempUID))
                {
                    endNode = null;
                    endNode = g.GetNode(tempUID);
                }
                else
                {
                    g.InsertNode(tempUID, endNode);
                }

                if ((ft_speed > 75.0) && (ft_speed <= 100.0)) ft_speed = 75.0;
                if ((tf_speed > 75.0) && (tf_speed <= 100.0)) tf_speed = 75.0;
                if ((ft_speed <= 0.0) || (ft_speed > 100.0)) ft_speed = speed;
                if ((tf_speed <= 0.0) || (tf_speed > 100.0)) tf_speed = speed;

                switch (tavel_dir)
                {
                    case "F":
                        startNode.AddNeighbor(new Neighbor(endNode.UID, seg_len, Convert.ToByte(ft_speed), privateRoad));
                        break;
                    case "T":
                        endNode.AddNeighbor(new Neighbor(startNode.UID, seg_len, Convert.ToByte(tf_speed), privateRoad));
                        break;
                    default:
                        startNode.AddNeighbor(new Neighbor(endNode.UID, seg_len, Convert.ToByte(ft_speed), privateRoad));
                        endNode.AddNeighbor(new Neighbor(startNode.UID, seg_len, Convert.ToByte(tf_speed), privateRoad));
                        break;
                }

                #endregion

                #region TeleAtlas
                /*
        // get and parse data from database
        //costs

        ft_cost = float.Parse(streetsReader["ft_cost"].ToString());
        tf_cost = float.Parse(streetsReader["tf_cost"].ToString());
        seg_len = float.Parse(streetsReader["seg_len"].ToString());

        //one-way & speed limitation
        one_way_opt = streetsReader["one_way"].ToString();
        speed = Convert.ToInt16(streetsReader["speed"]);

        //dynamap ID
        lineID = Convert.ToInt32(streetsReader["dynamap_id"]);

        // create start node and end node from the database data fields         

        //start node
        frlong = float.Parse(streetsReader["fromlong"].ToString());
        frlat = float.Parse(streetsReader["fromlat"].ToString());
        startNode = new Node(frlong, frlat);
        startNode = g.insertNode(startNode);

        tolong = float.Parse(streetsReader["tolong"].ToString());
        tolat = float.Parse(streetsReader["tolat"].ToString());
        endNode = new Node(tolong, tolat);
        endNode = g.insertNode(endNode);

        switch (one_way_opt)
        {
          case "FT":
            nb = new Neighbor(g.getID(endNode), seg_len, seg_len / speed);//--cause problem: id is not correct
            startNode.addNeighbor(nb);
            break;
          case "TF":
            nb = new Neighbor(g.getID(startNode), seg_len, seg_len / speed);
            endNode.addNeighbor(nb);
            break;
          default:
            nb = new Neighbor(g.getID(endNode), seg_len, seg_len / speed);
            startNode.addNeighbor(nb);
            nb = new Neighbor(g.getID(startNode), seg_len, seg_len / speed);
            endNode.addNeighbor(nb);
            break;
        }
        */
                #endregion

                if (i % 3000000 == 0) System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": " + g.NodeCount + ", " + i);
            }
            System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": Graph reading done.");
            streetsReader.Close();
            con.Close();
            streetsReader = null;

            #region Restrictions

            cmd.CommandText = "SELECT s1.fromlong, s1.fromlat, s1.tolong, s1.tolat, s2.fromlong, s2.fromlat, s2.tolong, s2.tolat, s1.dir_travel, s2.dir_travel, c.end_of_lk " +
                "from [" + tableName + "_RDMS] r1, [" + tableName + "_CDMS] c, [" + tableName + "_Streets] s1, [" + tableName + "_Streets] s2 " +
                "where (c.link_id = s1.link_id) and (s2.link_id = r1.man_linkid) and (c.cond_id = r1.cond_id) and (c.link_id = r1.link_id) and (c.ar_auto = 'Y') and " +
                "(s1.ar_auto = 'Y') and (s1.speed_cat <> ' ') and (s1.ferry_type = 'H') and (s2.ar_auto = 'Y') and " +
                "(s2.speed_cat <> ' ') and (s2.ferry_type = 'H') and ((c.cond_type = 3) or ((c.cond_type = 4) and (c.cond_val1 <> 'PERMISSION REQUIRED')) or (c.cond_type = 7)) " +
                "and (r1.cond_id not in (select r2.cond_id from [" + tableName + "_RDMS] r2 where r2.seq_number > 1))";
            con.Open();
            streetsReader = cmd.ExecuteReader();

            j = 0;
            GraphNode from1 = null, from2 = null, to1 = null, to2 = null;
            string dir1 = "", dir2 = "", end_of_lk = "";

            while (streetsReader.Read())
            {
                from1 = g.GetNode((new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(0)), Convert.ToDouble(streetsReader.GetDecimal(1)))).UID);
                to1 = g.GetNode((new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(2)), Convert.ToDouble(streetsReader.GetDecimal(3)))).UID);
                from2 = g.GetNode((new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(4)), Convert.ToDouble(streetsReader.GetDecimal(5)))).UID);
                to2 = g.GetNode((new GraphNode(Convert.ToDouble(streetsReader.GetDecimal(6)), Convert.ToDouble(streetsReader.GetDecimal(7)))).UID);
                dir1 = streetsReader.GetString(8);
                dir2 = streetsReader.GetString(9);
                end_of_lk = streetsReader.GetString(10);
                if (j % 400000 == 0) System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": " + j);
                j++;

                if ((from1 == null) || (from2 == null) || (to1 == null) || (to2 == null)) continue;

                if ((from1.Equals(from2)) && (to1.Equals(to2)))
                {
                    if (dir1 == "B")
                    {
                        if (end_of_lk == "R") from1.AddRestriction(to1.UID, to1.UID);
                        else to1.AddRestriction(from1.UID, from1.UID);
                    }
                }
                else if (from1.Equals(from2))
                {
                    if ((dir1 != "F") && (dir2 != "T"))
                    {
                        from2.AddRestriction(to1.UID, to2.UID);
                    }
                }
                else if (from1.Equals(to2))
                {
                    if ((dir1 != "F") && (dir2 != "F"))
                    {
                        to2.AddRestriction(to1.UID, from2.UID);
                    }
                }
                else if (to1.Equals(from2))
                {
                    if ((dir1 != "T") && (dir2 != "T"))
                    {
                        from2.AddRestriction(from1.UID, to2.UID);
                    }
                }
                else if (to1.Equals(to2))
                {
                    if ((dir1 != "T") && (dir2 != "F"))
                    {
                        to2.AddRestriction(from1.UID, from2.UID);
                    }
                }
            }
            #endregion

            streetsReader.Close();
            con.Close();
            System.Diagnostics.Debug.WriteLine(DateTime.Now.ToLongTimeString() + ": Graph restriction done.");

            cmd = null;
            streetsReader = null;
            con = null;

            GC.Collect();
            GC.WaitForPendingFinalizers();
            return g;
        }
        /*
		public static bool isWithin(float number, double small, double large)
		{
		  return (number > small && number < large) ? true : false;
		}
		*/
        public static Int16 GetSpeedFromSpeedCat(string speedCat)
        {
            Int16 speed = 5;
            switch (speedCat)
            {
                case "1":
                    speed = 75;
                    break;
                case "2":
                    speed = 75;
                    break;
                case "3":
                    speed = 60;
                    break;
                case "4":
                    speed = 50;
                    break;
                case "5":
                    speed = 35;
                    break;
                case "6":
                    speed = 25;
                    break;
                case "7":
                    speed = 15;
                    break;
                case "8":
                    speed = 5;
                    break;
            }
            return speed;
        }
    }
}