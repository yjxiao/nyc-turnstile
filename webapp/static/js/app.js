(function(){
    var app = angular.module('app', []);
    app.controller("DateController", function(){
	this.startdate = new Date(2013, 8, 1);
	this.enddate = new Date(2013, 8, 2);
    });
    app.controller("PanelController", function(){
	this.tab = 1;
	this.selectTab = function(setTab){
	    this.tab = setTab;
	};
	this.isSelected = function(checkTab){
	    return this.tab === checkTab;
	};
    });
})();
