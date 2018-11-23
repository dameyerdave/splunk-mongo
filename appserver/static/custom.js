// Require the Table view library
require([
    "splunkjs/mvc",
    "splunkjs/mvc/tableview",
    "splunkjs/mvc/simplexml/ready!"
], function(mvc, TableView) {


    // Inherit from the BaseCellRenderer base class
    var MyCustomCellRenderer = TableView.BaseCellRenderer.extend({
        initialize: function() {
            return;
        },
        canRender: function(cellData) {
            //console.log("cellData: ", cellData);
            return cellData.value && (cellData.field === 'user_profile_image_url' || cellData.field === 'entities_media_media_url');
        },
        setup: function($td, cellData) {
            return;
        },
        teardown: function($td, cellData) {
            return;
        },
        render: function($td, cellData) {
            $td.html("<img src='" + cellData.value + "' />");
            //console.log("cellData: ", cellData);
        }
    });

    mvc.Components.get('myCustomTable').getVisualization(function(tableView){
        tableView.table.addCellRenderer(new MyCustomCellRenderer());
        tableView.table.render();
    });

    // Create an instance of the custom cell renderer
    //var myCellRenderer = new MyCustomCellRenderer();

    //var myCustomTable = document.getElementById("myCustomTable");
    
    // Add the custom cell renderer to the table
    //myCustomTable.addCellRenderer(myCellRenderer); 

    // Render the table
    //myCustomTable.render();
   
});

