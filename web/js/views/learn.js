/* Level map, lesson and quiz views */
(function (FA) {
  'use strict';
  const h = FA.h;

  /* ---------- Home / level map ---------- */
  FA.views.home = function (main) {
    const st = FA.store;
    const firstOpen = FA.levels.find(l => st.isUnlocked(l.id) && !st.lvl(l.id).passed);
    const c = h('div', { class: 'container' });
    const passed = FA.levels.filter(l => st.lvl(l.id).passed).length;
    c.appendChild(h('div', { class: 'hero' },
      h('div', null,
        h('h1', null, 'Flink Academy'),
        h('div', { class: 'muted' }, 'From your first DataStream to reading the Flink dashboard like a senior engineer, with Apache Beam on Flink.')),
      h('div', { class: 'row' },
        firstOpen ? h('a', { class: 'btn primary', href: `#/level/${firstOpen.id}` }, `▶ Continue: Level ${firstOpen.id}`) : h('span', { class: 'pill ok' }, '🏆 All levels complete'),
        h('a', { class: 'btn', href: '#/lab' }, '🧪 Pipeline Lab'))));

    const pct = Math.round(100 * passed / FA.levels.length);
    c.appendChild(h('div', { class: 'card' },
      h('div', { class: 'row', style: { justifyContent: 'space-between' } },
        h('div', null, h('b', null, `${passed} of ${FA.levels.length} levels passed`), h('span', { class: 'muted' }, ` · ${st.state.xp} XP · ${st.rank().name}`)),
        h('span', { class: 'muted' }, 'Pass each quiz with ≥ 80% to unlock the next level')),
      h('div', { class: 'xpbar', style: { height: '10px', marginTop: '10px' } }, h('div', { style: { width: pct + '%' } }))));

    const tracks = {};
    FA.levels.forEach(l => { (tracks[l.track] = tracks[l.track] || []).push(l); });
    for (const [track, lvls] of Object.entries(tracks)) {
      c.appendChild(h('div', { class: 'track-title' }, track));
      const grid = h('div', { class: 'levels' });
      lvls.forEach(l => {
        const p = st.lvl(l.id), unlocked = st.isUnlocked(l.id);
        const cls = ['lvl', !unlocked && 'locked', p.passed && 'passed', firstOpen && firstOpen.id === l.id && 'current'].filter(Boolean).join(' ');
        grid.appendChild(h('div', { class: cls, onclick: () => unlocked ? FA.go(`#/level/${l.id}`) : FA.toast('🔒 Pass the previous level\'s quiz to unlock this one') },
          h('div', { class: 'icon' }, unlocked ? l.icon : '🔒'),
          h('div', { class: 'num' }, `Level ${l.id}`),
          h('h3', null, l.title),
          h('p', null, l.summary),
          h('div', { class: 'foot' },
            p.passed ? h('span', { class: 'pill ok' }, `✓ Passed · best ${p.best}%`) :
              p.attempts ? h('span', { class: 'pill warn' }, `Best ${p.best}% · ${p.attempts} attempt(s)`) :
                h('span', { class: 'pill' }, unlocked ? (p.read ? 'Lesson read · quiz pending' : 'Not started') : 'Locked'),
            h('span', { class: 'muted' }, `~${l.minutes} min`))));
      });
      c.appendChild(grid);
    }
    main.appendChild(c);
  };

  /* ---------- Lesson ---------- */
  FA.views.lesson = function (main, idStr) {
    const id = +idStr, lvl = FA.levels.find(l => l.id === id);
    if (!lvl) return FA.go('#/');
    if (!FA.store.isUnlocked(id)) { FA.toast('🔒 That level is still locked'); return FA.go('#/'); }
    const p = FA.store.lvl(id);
    const c = h('div', { class: 'container lesson' });
    c.appendChild(h('div', { class: 'lesson-head' },
      h('div', null, h('div', { class: 'muted' }, `${lvl.track} · Level ${lvl.id}`), h('h1', { style: { margin: '2px 0 0' } }, `${lvl.icon} ${lvl.title}`)),
      h('div', { class: 'row' },
        h('a', { class: 'btn ghost', href: '#/' }, '← Map'),
        h('a', { class: 'btn primary', href: `#/level/${id}/quiz` }, p.passed ? 'Retake quiz' : 'Take quiz →'))));
    if (lvl.objectives) c.appendChild(h('div', { class: 'card' }, h('b', null, 'By the end of this level you can:'),
      h('ul', { style: { margin: '6px 0 0' } }, lvl.objectives.map(o => h('li', null, o)))));

    const body = h('div');
    const toc = h('div', { class: 'toc' }, h('div', { class: 'muted', style: { marginBottom: '6px' } }, 'ON THIS PAGE'));
    lvl.sections.forEach((s, i) => {
      const anchor = `s${i}`;
      body.appendChild(h('h2', { id: anchor }, s.title));
      body.appendChild(h('div', { html: s.html }));
      toc.appendChild(h('a', { href: 'javascript:void 0', onclick: () => document.getElementById(anchor).scrollIntoView({ behavior: 'smooth' }) }, s.title));
    });
    if (lvl.refs && lvl.refs.length) {
      body.appendChild(h('h2', null, '📚 References for this level'));
      body.appendChild(h('ul', null, lvl.refs.map(r => h('li', null, h('a', { href: r[1], target: '_blank', rel: 'noopener' }, r[0])))));
    }
    const done = h('div', { class: 'card', style: { marginTop: '28px', textAlign: 'center' } },
      h('div', { style: { marginBottom: '10px' } }, p.read ? '✅ Lesson completed.' : 'Finished reading and trying the interactive parts?'),
      h('button', {
        class: 'btn primary', onclick: () => {
          if (!p.read) { p.read = true; FA.store.addXp(50, 'Lesson completed'); }
          FA.go(`#/level/${id}/quiz`);
        }
      }, p.read ? 'Go to quiz →' : 'Mark lesson complete & start quiz →'));
    body.appendChild(done);
    c.appendChild(h('div', { class: 'lesson-wrap' }, body, h('div', null, toc)));
    main.appendChild(c);
    FA.mountWidgets(c);
  };

  /* ---------- Quiz ---------- */
  FA.quizPool = function (lvl) {
    if (lvl.finalExam) {
      const pool = [];
      FA.levels.filter(l => !l.finalExam).forEach(l => FA.shuffle(l.quiz).slice(0, 2).forEach(q => pool.push(Object.assign({ from: l.title }, q))));
      return FA.shuffle(pool.concat(lvl.quiz)).slice(0, lvl.examSize || 25);
    }
    return FA.shuffle(lvl.quiz);
  };

  FA.views.quiz = function (main, idStr) {
    const id = +idStr, lvl = FA.levels.find(l => l.id === id);
    if (!lvl) return FA.go('#/');
    if (!FA.store.isUnlocked(id)) { FA.toast('🔒 Locked'); return FA.go('#/'); }
    const p = FA.store.lvl(id);
    if (!p.read && !lvl.finalExam) { FA.toast('Read the lesson first and mark it complete 📖'); return FA.go(`#/level/${id}`); }
    const questions = FA.quizPool(lvl).map(q => {
      const order = FA.shuffle(q.options ? q.options.map((_, i) => i) : []);
      return Object.assign({}, q, { order });
    });
    const answers = {};
    const c = h('div', { class: 'container' });
    c.appendChild(h('div', { class: 'lesson-head' },
      h('div', null, h('div', { class: 'muted' }, `Level ${id} quiz · ${questions.length} questions · pass mark 80%`), h('h1', { style: { margin: '2px 0 14px' } }, `${lvl.icon} ${lvl.title}: Quiz`)),
      h('a', { class: 'btn ghost', href: `#/level/${id}` }, '← Back to lesson')));

    const qEls = questions.map((q, qi) => {
      const box = h('div', { class: 'q' });
      box.appendChild(h('div', { class: 'qn' }, `Question ${qi + 1}${q.type === 'multi' ? ' · select ALL that apply' : q.type === 'number' ? ' · enter a number' : ''}${q.from ? ' · from: ' + q.from : ''}`));
      box.appendChild(h('div', { class: 'qt', html: q.q }));
      if (q.code) box.appendChild(h('pre', null, h('code', null, q.code)));
      if (q.type === 'number') {
        const inp = h('input', { type: 'number', step: 'any', oninput: e => { answers[qi] = e.target.value; } });
        box.appendChild(inp);
      } else {
        q.order.forEach(oi => {
          const multi = q.type === 'multi';
          const inp = h('input', { type: multi ? 'checkbox' : 'radio', name: 'q' + qi });
          const opt = h('label', { class: 'opt' }, inp, h('span', { html: q.options[oi] }));
          inp.addEventListener('change', () => {
            if (multi) {
              const set = new Set(answers[qi] || []);
              inp.checked ? set.add(oi) : set.delete(oi); answers[qi] = [...set];
            } else answers[qi] = oi;
            box.querySelectorAll('.opt').forEach(o => o.classList.toggle('sel', o.querySelector('input').checked));
          });
          opt.dataset.oi = oi;
          box.appendChild(opt);
        });
      }
      c.appendChild(box);
      return box;
    });

    const result = h('div');
    const submit = h('button', {
      class: 'btn primary', onclick: () => {
        const unanswered = questions.filter((q, i) => answers[i] === undefined || (Array.isArray(answers[i]) && !answers[i].length) || answers[i] === '').length;
        if (unanswered && !confirm(`${unanswered} question(s) unanswered. Submit anyway?`)) return;
        let correct = 0;
        questions.forEach((q, i) => {
          const a = answers[i];
          let ok;
          if (q.type === 'multi') { const want = new Set(q.answer); const got = new Set(a || []); ok = want.size === got.size && [...want].every(x => got.has(x)); }
          else if (q.type === 'number') { const tol = q.tolerance || 0; ok = a !== undefined && a !== '' && Math.abs(parseFloat(a) - q.answer) <= tol; }
          else ok = a === q.answer;
          if (ok) correct++;
          const box = qEls[i];
          box.querySelectorAll('input').forEach(x => x.disabled = true);
          box.querySelectorAll('.opt').forEach(o => {
            const oi = +o.dataset.oi;
            const isRight = q.type === 'multi' ? q.answer.includes(oi) : q.answer === oi;
            if (isRight) o.classList.add('right');
            else if (o.querySelector('input').checked) o.classList.add('wrong');
          });
          box.appendChild(h('div', { class: 'explain', html: `${ok ? '✅ <b>Correct.</b>' : '❌ <b>Not quite.</b>'} ${q.type === 'number' ? `Answer: <b>${q.answer}</b>. ` : ''}${q.explain || ''}` }));
          box.style.borderColor = ok ? 'rgba(51,196,129,.6)' : 'rgba(239,75,75,.6)';
        });
        const pct = Math.round(100 * correct / questions.length);
        const wasPassed = p.passed;
        p.attempts++; p.best = Math.max(p.best, pct);
        const pass = pct >= 80;
        if (pass && !wasPassed) { p.passed = true; FA.store.addXp(lvl.finalExam ? 500 : 150 + (pct === 100 ? 50 : 0), pct === 100 ? 'Perfect score!' : 'Level passed'); }
        else FA.store.save();
        submit.remove();
        const next = FA.levels[FA.levels.findIndex(l => l.id === id) + 1];
        result.appendChild(h('div', { class: 'card', style: { textAlign: 'center', borderColor: pass ? 'var(--ok)' : 'var(--bad)' } },
          h('div', { class: 'score', style: { color: pass ? 'var(--ok)' : 'var(--bad)' } }, `${pct}%`),
          h('div', null, `${correct} / ${questions.length} correct · ${pass ? (wasPassed ? 'Passed again 💪' : '🎉 Level passed! ' + (next ? `Level ${next.id} unlocked.` : 'Course complete!')) : 'You need 80% to pass. Review the explanations above and the lesson, then try again (questions and options are reshuffled).'}`),
          h('div', { class: 'row', style: { justifyContent: 'center', marginTop: '12px' } },
            h('button', { class: 'btn', onclick: () => FA.render() }, '↻ Retry quiz'),
            h('a', { class: 'btn', href: `#/level/${id}` }, '📖 Review lesson'),
            pass && next ? h('a', { class: 'btn primary', href: `#/level/${next.id}` }, `Level ${next.id} →`) : null)));
        result.scrollIntoView({ behavior: 'smooth' });
      }
    }, 'Submit answers');
    c.appendChild(h('div', { style: { textAlign: 'center', margin: '20px 0' } }, submit));
    c.appendChild(result);
    main.appendChild(c);
  };
})(window.FA);
