var DBClientMock = function () {
    this.createSpy = function () {
        var connectionSpy = jasmine.createSpyObj('DBClient', ['execute']);
        return {
            getConnection: function(){
                return connectionSpy;
            }
        };
    };
};

module.exports = new DBClientMock();