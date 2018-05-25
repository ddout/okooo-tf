# -*- coding: utf-8 -*-
import numpy

from mappers.pgsqlwares import Pgsql


class PlaysMapper(object):

    def __init__(self):
        self.__pgsql = Pgsql()

    def playlist_to_matrix(self, play_list):
        matrix_x = []
        matrix_y = []
        for item in play_list:
            odds_info = item.get("odds_info")
            play_result = item.get("play_result")
            #
            matrix_x_child = []
            _y = numpy.zeros(dtype=float, shape=3)
            if play_result == 3:
                _y[0] = 1.
            else:
                if play_result == 1:
                    _y[1] = 1.
                else:
                    _y[2] = 1.
            matrix_y.append(_y)
            #
            for i in odds_info:
                start = i.get("Start", {})
                end = i.get("End", {})
                # home,away,draw
                matrix_x_child.append(
                    [float(start["home"]), float(start["away"]), float(start["draw"]),
                     float(end["home"]), float(end["away"]), float(end["draw"])])
            if len(matrix_x_child) < 4:
                for v in range(0, 4 - len(matrix_x_child)):
                    matrix_x_child.append([0.00, 0.00, 0.00, 0.00, 0.00, 0.00])
            if len(matrix_x_child) > 4:
                matrix_x_child = matrix_x_child[0:4]
            matrix_x.append(matrix_x_child)
            #
        return numpy.array(matrix_x), numpy.array(matrix_y)

    def train_list(self, page=0, limit=10):
        sql = """
                select 
                    id,team_home,team_vis,
                    play_result,odds_info
                from okooo.play 
                where play_result is not null
                  and jsonb_array_length(odds_info) > 0
                  and id >= 150000
                order by id asc
                limit %(limit)s offset %(offset)s
                ;
              """
        #
        v_offset = page * limit
        v_limit = limit
        #
        params = {"offset": v_offset, "limit": v_limit}
        play_list = self.__pgsql.getAll(sql, **params)
        return self.playlist_to_matrix(play_list)

    def test_list(self, page=0, limit=10):
        sql = """
                select 
                    id,team_home,team_vis,
                    play_result,odds_info
                from okooo.play 
                where play_result is not null
                  and jsonb_array_length(odds_info) > 0
                  and id < 150000
                order by id asc
                limit %(limit)s offset %(offset)s
                ;
              """
        #
        v_offset = page * limit
        v_limit = limit
        #
        params = {"offset": v_offset, "limit": v_limit}
        play_list = self.__pgsql.getAll(sql, **params)
        return self.playlist_to_matrix(play_list)
