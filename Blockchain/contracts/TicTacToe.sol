//SPDX-License-Identifier: Unlicense
pragma solidity ^0.4.24;

/**
 * @title TicTacToe contract
 **/
contract TicTacToe {
    address[2] public players;

    /**
     turn
     1 - players[0]'s turn
     2 - players[1]'s turn
     */
    uint public turn = 1;

    /**
     status
     0 - ongoing
     1 - players[0] won
     2 - players[1] won
     3 - draw
     */
    uint public status;

    /**
    board status
     0    1    2
     3    4    5
     6    7    8
     */
    uint[9] private board;

    /**
      * @dev Deploy the contract to create a new game
      * @param opponent The address of player2
      **/
    constructor(address opponent) public {
        require(msg.sender != opponent, "No self play");
        players = [msg.sender, opponent];
    }

    /**
      * @dev Check a, b, c in a line are the same
      * _threeInALine doesn't check if a, b, c are in a line
      * @param a position a
      * @param b position b
      * @param c position c
      **/    
    function _threeInALine(uint a, uint b, uint c) private view returns (bool){
        /*Please complete the code here.*/
        if (board[a]!=0){
          return ((board[a]==board[b]) && (board[b]==board[c]));
        }
        else {
          return false;
        }
    }

    /**
     * @dev get the status of the game
     * @param pos the position the player places at
     * @return the status of the game
     */
    function _getStatus(uint pos) private view returns (uint) {
        /*Please complete the code here.*/
        // Check horizontal, vertical and diagonal lines
        if (pos==0 || pos==8){
          if (_threeInALine(3*uint(pos/3), 3*uint(pos/3)+1, 3*uint(pos/3)+2)
            || _threeInALine(pos%9, (pos+3)%9, (pos+6)%9)
            || _threeInALine(0, 4, 8)){
            return 3-turn;
          }
        }
        else if (pos==2 || pos==6){
          if (_threeInALine(3*uint(pos/3), 3*uint(pos/3)+1, 3*uint(pos/3)+2)
            || _threeInALine(pos%9, (pos+3)%9, (pos+6)%9)
            || _threeInALine(2, 4, 6)){
            return 3-turn;
          }
        }
        else if (pos==4){
          if (_threeInALine(3*uint(pos/3), 3*uint(pos/3)+1, 3*uint(pos/3)+2)
            || _threeInALine(pos%9, (pos+3)%9, (pos+6)%9)
            || _threeInALine(0, 4, 8)
            || _threeInALine(2, 4, 6)){
            return 3-turn;
          }
        }
        else {
          if (_threeInALine(3*(pos/3), 3*(pos/3)+1, 3*(pos/3)+2)
            || _threeInALine(pos%9, (pos+3)%9, (pos+6)%9)){
            return 3-turn;
          }
        }
        // Check if the game is drawn
        uint count = 0;
        for (uint j = 0; j < 9; j++) {
            if (board[j] != 0){
              count++;
            }
        }
        if (count==9){
          return 3;
        }
        // You can still play
        else {
          return 0;
        }
    }

    /**
     * @dev ensure the game is still ongoing before a player moving
     * update the status of the game after a player moving
     * @param pos the position the player places at
     */
    modifier _checkStatus(uint pos) {
        /*Please complete the code here.*/
        require(status==0);
        _;
        status = _getStatus(pos);
    }

    /**
     * @dev check if it's msg.sender's turn
     * @return true if it's msg.sender's turn otherwise false
     */
    function myTurn() public view returns (bool) {
       /*Please complete the code here.*/
       if (players[turn-1]==msg.sender){
         return true;
       }
       else {
         return false;
       }
    }

    /**
     * @dev ensure it's a msg.sender's turn
     * update the turn after a move
     */
    modifier _myTurn() {
      /*Please complete the code here.*/
      require(myTurn());
      _;
    }

    /**
     * @dev check a move is valid
     * @param pos the position the player places at
     * @return true if valid otherwise false
     */
    function validMove(uint pos) public view returns (bool) {
      /*Please complete the code here.*/
      if (((pos <= 8) && (pos >= 0)) && (_getStatus(pos)==0)){
        return (board[pos]==0);
      }
      else {
        return false;
      }
    }

    /**
     * @dev ensure a move is valid
     * @param pos the position the player places at
     */
    modifier _validMove(uint pos) {
      /*Please complete the code here.*/
      require(validMove(pos));
      _;
    }

    /**
     * @dev a player makes a move
     * @param pos the position the player places at
     */
    function move(uint pos) public _validMove(pos) _checkStatus(pos) _myTurn {
        board[pos] = turn;
        turn = 3-turn;
    }

    /**
     * @dev show the current board
     * @return board
     */
    function showBoard() public view returns (uint[9]) {
      return board;
    }
}
