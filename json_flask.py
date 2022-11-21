# -*- coding: utf-8 -*-
from flask import Flask, jsonify
from json_response import JsonResponse


class JsonFlask(Flask):
    def make_response(self, rv):
        """return: list、dict、None"""
        if rv is None or isinstance(rv, (list, dict)):
            rv = JsonResponse.success(rv)
        if isinstance(rv, JsonResponse):
            rv = jsonify(rv.to_dict())
        return super().make_response(rv)


