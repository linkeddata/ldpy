#!/usr/bin/env python
from RDF import NS, Uri, Node, Statement, Storage, Model, SPARQLQuery, Serializer, Parser

class ns(object):
    rdf  = NS('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
    rdfs = NS('http://www.w3.org/2000/01/rdf-schema#')
    owl  = NS('http://www.w3.org/2002/07/owl#')
    acl  = NS('http://www.w3.org/ns/auth/acl#')
    cert = NS('http://www.w3.org/ns/auth/cert#')
    stat = NS('http://www.w3.org/ns/posix/stat#')
    foaf = NS('http://xmlns.com/foaf/0.1/')
    dc   = NS('http://purl.org/dc/elements/1.1/')
    cc   = NS('http://creativecommons.org/ns#')

from rdflib_sparql import processor as sparqlUpdate

from flask import Flask, Response, request, abort
app = Flask(__name__)
app.debug = True

import os, stat

class Graph(object):
    TYPEMAP = {
        'turtle': 'turtle',
        'ttl': 'turtle',
        'json': 'json',
        'rdf+xml': 'rdfxml-abbrev',
        'rdf': 'rdfxml-abbrev',
        'nt': 'ntriples',
        'ntriples': 'ntriples',
        'n3': 'turtle',
    }

    def _h(self, code, level, facility, message, line, column, byte, file, uri):
        raise Exception(message)

    def __init__(self, p, base_uri=None):
        self._base = base_uri or request.base_url
        self._f = self._f0 = 'turtle'
        self._p = os.path.abspath(p)
        if p and p[-1]=='/' and (not self._p or self._p[-1]!='/'):
            self._p += '/'
        self._p0 = self._p
        if not self.exists():
            for k in self.TYPEMAP:
                if self.exists(p + '.' + k):
                    self._p0 += '.' + k
                    self._f0 = self.TYPEMAP[k]
                    break
        self._s = Storage(storage_name='hashes', name='', options_string="hash-type='memory'")
        self._g = Model(self._s)
        self._editable = False

    def headers(self, more=None):
        d = {
            'Base': self._base,
            'Storage-Format': self._f0,
            'Storage-Path': self._p0,
            'Triples': len(self._g),
        }
        if self._editable:
            d['MS-Author-Via'] = 'DAV, SPARQL'
            d['DAV'] = '1'
        if more:
            d.update(more)
        return d

    def exists(self, p=None):
        return os.path.exists(p or self._p0)

    def load(self, uri='', name=''):
        if uri:
            return self._g.load(uri=uri, name=name, handler=self._h)
        st = os.stat(self._p0)
        if not stat.S_ISDIR(st.st_mode):
            self._editable = True
            p = Parser(self._f0)
            assert p.parse_into_model(self._g, 'file:' + self._p0, base_uri=self._base, handler=self._h)
        else:
            self._g.append(Statement(Node(uri_string=self._base),ns.rdf['type'],ns.stat['Directory']))
            for k in os.listdir(self._p0):
                st = os.stat(self._p0 + '/' + k)
                if stat.S_ISDIR(st.st_mode):
                    kn = Node(uri_string=str(k)+'/')
                    self._g.append(Statement(kn,ns.rdf['type'],ns.stat['Directory']))
                else:
                    kn = Node(uri_string=str(k))
                self._g.append(Statement(Node(uri_string=self._base),ns.rdfs['member'],kn))
                self._g.append(Statement(kn,ns.stat['atime'],Node(literal=str(int(st.st_atime)))))
                self._g.append(Statement(kn,ns.stat['ctime'],Node(literal=str(int(st.st_ctime)))))
                self._g.append(Statement(kn,ns.stat['mtime'],Node(literal=str(int(st.st_mtime)))))
                self._g.append(Statement(kn,ns.stat['size'],Node(literal=str(int(st.st_size)))))

    def append(self, s, name=None, mime_type=None):
        p = Parser(name=name, mime_type=mime_type)
        assert p.parse_string_into_model(self._g, s, self._base, handler=self._h)

    def update(self, s, name=None, mime_type=None):
        if mime_type == 'application/sparql-update':
            for op in sparqlUpdate.translateUpdate(sparqlUpdate.parseUpdate(s)):
                if op.name is 'InsertData':
                    g = Graph('sparql:')
                    for elt in op.triples:
                        g.append(' '.join([x.n3() for x in elt]), name='turtle')
                    for x in g._g:
                        self._g.append(x)
                elif op.name is 'DeleteData':
                    g = Graph('sparql:')
                    for elt in op.triples:
                        g.append(' '.join([x.n3() for x in elt]), name='turtle')
                    for x in g._g:
                        if x in self._g:
                            del self._g[x]
        else:
            return self.append(s, name, mime_type)

    def toString(self, f):
        if f == 'html':
            return '''<!DOCTYPE html><html><head>
<link type="text/css" rel="stylesheet" href="https://w3.scripts.mit.edu/tabulator/tabbedtab.css" />
<script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.7.1/jquery.min.js"></script>
<script type="text/javascript" src="https://w3.scripts.mit.edu/tabulator/js/mashup/mashlib.js"></script>
<script type="text/javascript">
jQuery.ajaxPrefilter(function(options) { if (options.crossDomain) { options.url = "https://w3.scripts.mit.edu/proxy?uri=" + encodeURIComponent(options.url); } });
jQuery(document).ready(function() { tabulator.outline.GotoSubject(tabulator.kb.sym(window.location.href), true, undefined, true, undefined); });
</script></head><body>
<div class="TabulatorOutline" id="DummyUUID"><table id="outline"></table></div></body></html>'''
        s = Serializer(name=f)
        s.set_feature(Uri('http://feature.librdf.org/raptor-writeBaseURI'), Node(literal='0')._node)
        for k, v in list(ns.__dict__.items()):
            if type(v) is NS:
                s.set_namespace(k, Uri(v._prefix))
        return s.serialize_model_to_string(self._g, base_uri=self._base)

    def save(self):
        s = self.toString(self._f0)
        file(self._p0, 'w').write(s)

    def size(self):
        return self._g.size()

    def unlink(self):
        if self.size() > 0:
            return os.unlink(self._p0)

    def query(self, query):
        q = SPARQLQuery(query, base_uri=self._base)
        r = q.execute(self._g)
        for mtype, q in request.accept_mimetypes:
            for k in ('json', 'xml', 'tsv', 'csv'):
                if k in mtype:
                    return Response(r.to_string('http://www.w3.org/ns/formats/SPARQL_Results_'+k.upper(), base_uri=self._base))
        return Response(r.to_string('http://www.w3.org/ns/formats/SPARQL_Results_JSON', base_uri=self._base))

    def __call__(self, status=None, headers=None, body=None):
        f = 'html'
        m = 'text/html'
        for mtype, q in request.accept_mimetypes:
            elt = mtype.split('/', 1) + ['']
            if elt[1] in self.TYPEMAP:
                f = self.TYPEMAP[elt[1]]
                m = mtype
                break
        if body is None and (status is None or status < 400):
            body = self.toString(f)
        return Response(body, status=status, headers=self.headers(headers), mimetype=m)

@app.route('/')
@app.route('/<path:graph>')
def httpRead(graph=''):
    g = Graph(graph)
    if not g.exists():
        return g(status=404)
    g.load()
    return g()

@app.route('/<path:graph>', methods=('POST', 'PUT'))
def httpWrite(graph):
    d = request.data
    if request.content_type:
        g = Graph(graph)
        if request.method != 'PUT':
            if g.exists():
                g.load()
        if d:
            g.update(d, mime_type=request.content_type)
        g.save()
        return g(status=204, body='')
    return g(status=415)

@app.route('/<path:graph>', methods=('DELETE',))
def httpDELETE(graph):
    g = Graph(graph)
    if not g.exists():
        return g(status=404)
    g.unlink()
    if g.exists():
        return g(status=409)
    return g(status=204, body='')

@app.route('/<path:graph>', methods=('MKCOL',))
def httpMKCOL(graph):
    g = Graph(graph)
    if g.exists():
        return g(status=409)
    os.makedirs(graph)
    if not g.exists():
        return g(status=404)
    g.load()
    return g(status=201)

def application(app):
    def handler(env, respond):
        if 'PATH_INFO' in env and env['PATH_INFO'] and 'SCRIPT_NAME' in env:
            env['SCRIPT_NAME'] = os.path.dirname(env['SCRIPT_NAME'])
        return app(env, respond)
    return handler
app = application = application(app)

if __name__ == '__main__':
    from flup.server.fcgi import WSGIServer
    WSGIServer(app).run()
