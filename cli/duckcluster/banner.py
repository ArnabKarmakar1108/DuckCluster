"""Startup banner for the interactive shell."""

from __future__ import annotations

_DUCK_ASCII = r"""
           ########
          #####  ####
         ####      ###
        ###       ####
        ###       #####
        ##        ########
        ##         #######
##      ###        #######
###     ###        #####
#####    ##         ###
## ########          ###
##   #####            ###
##  ###                ###
##  ################    ##
###  #########    ###   ##
 ##   ###          ##   ##
 ###               ##   ##
  ###                  ##
   ####              ####
     ####           ####
"""

_TITLE = "DuckCluster interactive shell"


def print_banner() -> None:
    print(_DUCK_ASCII.rstrip())
    print(_TITLE)
    print()
