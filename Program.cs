using System;
using System.Linq;
using System.Collections.Generic;
using Xbim.Ifc;
using Xbim.Ifc4.Interfaces;
using Xbim.IO;
using Neo4j.Driver;

namespace neo4j.bim
{
    class Program
    {
        static void Main(string[] args)
        {   
            const string fileName = "AC20-FZK-Haus.ifc";
            var editor = new XbimEditorCredentials
            {
                ApplicationDevelopersName = "Developer Name",
                ApplicationFullName = "Application Name",
                ApplicationIdentifier = "Application ID",
                ApplicationVersion = "4.0",
                EditorsFamilyName = "Editor Name",
                EditorsGivenName = "Editor Name",
                EditorsOrganisationName = "Editor Organisation"
            };

            Console.WriteLine("Loading: " + fileName);

            using (var model = IfcStore.Open(fileName, editor, null, null, XbimDBAccess.Read, -1))
            using (var driver = new Neo4jDriver("neo4j://localhost", "neo4j", "test1234"))
            {
            
                var roots = model.Instances.OfType<IIfcRoot>();
                foreach (var root in roots) {
                    if (root is IIfcPropertyDefinition) {
                         Console.WriteLine( string.Format("Prop: {0} {1}: {2}",root.GlobalId, root.GetType(), root.Name ));
                    } else if (root is IIfcRelationship) {
                        Console.WriteLine( string.Format("Rel: {0} {1}: {2}",root.GlobalId, root.GetType(), root.Name ));
                    } else if (root is IIfcObjectDefinition) {
                        Console.WriteLine( string.Format("Obj: {0} {1}: {2}",root.GlobalId, root.GetType(), root.Name ));
                    } else {
                        Console.WriteLine( string.Format("N/A: {1} {2}: {3}",root.GlobalId, root.GetType(), root.Name ));
                    }
                }
                Console.WriteLine(string.Format("Number of roots: {0}", roots.Count()));

                           
                List<Node> objects = model.Instances
                .OfType<IIfcRoot>()
                .OfType<IIfcObjectDefinition>()
                .Select(r => 
                new Node(
                    r.GlobalId.ToString(),
                    new Dictionary<string, object>() 
                    {
                        { "type", r.GetType().ToString() }, 
                        { "name", r.Name.ToString() },
                        { "description", r.Description.ToString() },
                        { "entityLabel", r.EntityLabel }
                        // LongName
                        // Height
                        // GrossFloorArea
                        // GrossPerimeter
                    }
                    )
                )
                .ToList();
                Console.WriteLine(string.Format("Number of objects: {0}", objects.Count()));
                driver.writeObjects(objects);

                List<Node> spaces = model.Instances
                .OfType<IIfcRoot>()
                .OfType<IIfcSpace>()
                .Select(r => 
                new Node(
                    r.GlobalId.ToString(),
                    new Dictionary<string, object>() {
                        { "type", r.GetType().ToString() }, 
                        { "name", r.Name.ToString() },
                        { "description", r.Description.ToString() },
                        { "entityLabel", r.EntityLabel },
                        { "LongName", r.LongName.ToString() }
                        
                        // LongName
                        // Height
                        // GrossFloorArea
                        // GrossPerimeter
                    }
                    )
                )
                .ToList();
                Console.WriteLine(string.Format("Number of spaces: {0}", spaces.Count()));
                driver.writeObjects(spaces);

                // ------------------------------------------------------------
                // Not sure if properties should be retrieved like this
                // ------------------------------------------------------------
                // Dictionary<string, Node> propertyDict = model.Instances
                // .OfType<IIfcRoot>()
                // .OfType<IIfcPropertyDefinition>()
                // .ToDictionary( r => r.GlobalId.ToString(), r => new Node(
                //     r.GlobalId.ToString(),
                //     new Dictionary<string, object>() {
                //         { "type", r.GetType().ToString() }, 
                //         { "name", r.Name.ToString() }, 
                //         { "description", r.Description.ToString() },
                //         { "entityLabel", r.EntityLabel }
                //     }
                //     )
                // );
                // Console.WriteLine(string.Format("Number of properties: {0}", propertyDict.Count()));
                // driver.writeProperties(objectDict.Values.ToList());


                // ------------------------------------------------------------
                // These are the rel types found in the exampe file
                // ------------------------------------------------------------
                // Xbim.Ifc4.SharedBldgElements.IfcRelConnectsPathElements
                // Xbim.Ifc4.ProductExtension.IfcRelAssociatesMaterial
                // - Xbim.Ifc4.ProductExtension.IfcRelContainedInSpatialStructure
                // Xbim.Ifc4.ProductExtension.IfcRelFillsElement
                // Xbim.Ifc4.ProductExtension.IfcRelSpaceBoundary
                // Xbim.Ifc4.ProductExtension.IfcRelVoidsElement
                // - Xbim.Ifc4.Kernel.IfcRelAggregates
                // Xbim.Ifc4.Kernel.IfcRelAssociatesClassification
                // Xbim.Ifc4.Kernel.IfcRelDefinesByProperties
                // Xbim.Ifc4.Kernel.IfcRelDefinesByType

                List<Rel> relContainedInSpatialStructure = model.Instances
                .OfType<IIfcRoot>()
                .OfType<IIfcRelContainedInSpatialStructure>()
                .SelectMany( r => r.RelatedElements, (r, related) => new {r, related})
                .Select( 
                    pair => new Rel(
                    pair.r.GlobalId.ToString(),
                    pair.r.RelatingStructure.GlobalId,
                    pair.related.GlobalId.ToString(),
                    new Dictionary<string, object>() {
                        { "type", pair.r.GetType().ToString() },
                        { "name", pair.r.Name.ToString() },
                        { "description", pair.r.Description.ToString() },
                        { "entityLabel", pair.r.EntityLabel }
                    }
                    )
                ).ToList();
                Console.WriteLine(string.Format("Number of RelContainedInSpatialStructure: {0}", relContainedInSpatialStructure.Count()));
                driver.writeRelContainedInSpatialStructure(relContainedInSpatialStructure);


                List<Rel> relRelAggregates = model.Instances
                .OfType<IIfcRoot>()
                .OfType<IIfcRelAggregates>()
                .SelectMany( r => r.RelatedObjects, (r, related) => new {r, related})
                .Select( 
                    pair => new Rel(
                    pair.r.GlobalId.ToString(),
                    pair.r.RelatingObject.GlobalId,
                    pair.related.GlobalId.ToString(),
                    new Dictionary<string, object>() {
                        { "type", pair.r.GetType().ToString() },
                        { "name", pair.r.Name.ToString() },
                        { "description", pair.r.Description.ToString() },
                        { "entityLabel", pair.r.EntityLabel }
                    }
                    )
                ).ToList();
                Console.WriteLine(string.Format("Number of RelAggregates: {0}", relRelAggregates.Count()));
                driver.writeRelAggegates(relRelAggregates);               
            }

        }
    }

    class Node {
        public string id {get; set;}
        public Dictionary<string, Object> properties { get; set;}

        public Node(string id, Dictionary<string, Object> properties) {
            this.id = id;
            this.properties = properties;
        }
    }

    class Rel {
        public string from {get; set;}
        public string to {get; set;}
        public string id {get; set;}
        public Dictionary<string, Object> properties { get; set;}

        public Rel(string id, string from, string to, Dictionary<string, Object> properties) {
            this.id = id;
            this.from = from;
            this.to = to;
            this.properties = properties;
        }
    }

    class Neo4jDriver : IDisposable {
        public IDriver Driver { get; }

        public Neo4jDriver(string uri, string user, string password)
        {
            Driver = GraphDatabase.Driver(uri, AuthTokens.Basic(user, password));
            createIndex();
        }

        private void createIndex() {
            using (var session = Driver.Session()) {
                try 
                {
                    session.WriteTransaction( tx => 
                        tx.Run("CREATE CONSTRAINT objectIndex ON (n:Object) ASSERT (n.id) IS NODE KEY")
                    );
                } 
                catch (Exception e)
                {
                    Console.WriteLine(e.Message.ToString());
                }
                try 
                {
                    session.WriteTransaction( tx => 
                        tx.Run("CREATE CONSTRAINT propIndex ON (n:Prop) ASSERT (n.id) IS NODE KEY")
                    );
                } 
                catch (Exception e)
                {
                    Console.WriteLine(e.Message.ToString());
                }
            }
        }

        public void Dispose()
        {
            Driver?.Dispose();
        }

        public void writeObjects(List<Node> objects)
        {
           using (var session = Driver.Session()) {
                session.WriteTransaction( tx => 
                    tx.Run("UNWIND $objects as object MERGE (o:Object{id: object.id}) set o += object.properties", new {objects})
                );            
           }            
        }

        public void writeProperties(List<Node> props)
        {
           using (var session = Driver.Session()) {
                session.WriteTransaction( tx => 
                    tx.Run("UNWIND $props as prop MERGE (p:Prop{id: prop.id}) set p += prop.properties", new {props})
                );            
           }            
        }

        public void writeRelContainedInSpatialStructure(List<Rel> rels)
        {
           using (var session = Driver.Session()) {
                session.WriteTransaction( tx => 
                    tx.Run("UNWIND $rels as rel MATCH (from:Object{id: rel.from}), (to:Object{id: rel.to}) MERGE (from)-[r:CONTAINES_SPATIAL_STUCTURE{id:rel.id}]->(to) set r += rel.properties", new {rels})
                );            
           }            
        }

        public void writeRelAggegates(List<Rel> rels)
        {
           using (var session = Driver.Session()) {
                session.WriteTransaction( tx => 
                    tx.Run("UNWIND $rels as rel MATCH (from:Object{id: rel.from}), (to:Object{id: rel.to}) MERGE (from)-[r:AGGREGATES{id:rel.id}]->(to) set r += rel.properties", new {rels})
                );            
           }            
        }

    }
}
