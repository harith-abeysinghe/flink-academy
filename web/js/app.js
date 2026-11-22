/* Routes + boot */
(function (FA) {
  'use strict';
  FA.route('/', m => FA.views.home(m), 'home');
  FA.route('/level/(\\d+)', (m, id) => FA.views.lesson(m, id), 'home');
  FA.route('/level/(\\d+)/quiz', (m, id) => FA.views.quiz(m, id), 'home');
  FA.route('/lab', m => FA.views.lab(m), 'lab');
  FA.route('/lab/([a-z-]+)', (m, id) => FA.views.lab(m, id), 'lab');
  FA.route('/config', m => FA.views.config(m), 'config');
  FA.route('/cluster', m => FA.views.cluster(m), 'cluster');
  FA.route('/glossary', m => FA.views.glossary(m), 'glossary');
  FA.route('/references', m => FA.views.references(m), 'references');
  FA.route('/progress', m => FA.views.progress(m), 'progress');
  FA.start();
})(window.FA);
