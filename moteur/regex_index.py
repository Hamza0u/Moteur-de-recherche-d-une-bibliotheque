ANY = "__ANY__"


class State:
    def __init__(self):
        self.eps = []   # epsilon transitions
        self.edges = {} # transitions sur caractères
        self.end = False


def connect(a, b, c=None):
    if c is None:
        a.eps.append(b)
    else:
        a.edges.setdefault(c, []).append(b)


class RegExTree:
    def __init__(self, root, subtrees=None):
        self.root = root
        self.subtrees = subtrees if subtrees else []

    def __str__(self):
        if not self.subtrees:
            return self.root
        return f"{self.root}({','.join(str(t) for t in self.subtrees)})"


def check_and_prepare(regex):
    """Validation simplifiée de la regex utilisateur."""
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789|.*()")
    if not regex:
        return None
    if any(c not in allowed for c in regex):
        # caractère non autorisé : on rejette
        return None
    if regex.count('(') != regex.count(')'):
        # parenthèses non équilibrées
        return None
    return regex


def parse_regex_to_tree(regex):
    """Parsing récursif de la regex vers un arbre syntaxique."""

    def parse_expr(i):
        left, i = parse_term(i)
        while i < len(regex) and regex[i] == '|':
            right, i = parse_term(i + 1)
            left = RegExTree('|', [left, right])
        return left, i

    def parse_term(i):
        if i >= len(regex) or regex[i] in ')|':
            return RegExTree('ε'), i
        left, i = parse_factor(i)
        while i < len(regex) and regex[i] not in ')|':
            right, i = parse_factor(i)
            left = RegExTree('.', [left, right])
        return left, i

    def parse_factor(i):
        base, i = parse_base(i)
        while i < len(regex) and regex[i] == '*':
            base = RegExTree('*', [base])
            i += 1
        return base, i

    def parse_base(i):
        if regex[i] == '(':
            node, i = parse_expr(i + 1)
            i += 1  # sauter ')'
            return node, i
        return RegExTree(regex[i]), i + 1

    tree, _ = parse_expr(0)
    return tree


def tree_to_nfa(node):
    """Construction de l'automate NFA (Thompson) à partir de l'arbre."""
    # Feuille
    if not node.subtrees:
        s, e = State(), State()
        if node.root == 'ε':
            connect(s, e)
        elif node.root == '.':
            # '.' en feuille = caractère quelconque
            connect(s, e, ANY)
        else:
            connect(s, e, node.root)
        return s, e

    # Concaténation
    if node.root == '.':
        s1, e1 = tree_to_nfa(node.subtrees[0])
        s2, e2 = tree_to_nfa(node.subtrees[1])
        connect(e1, s2)
        return s1, e2

    # Alternance '|'
    if node.root == '|':
        s, e = State(), State()
        s1, e1 = tree_to_nfa(node.subtrees[0])
        s2, e2 = tree_to_nfa(node.subtrees[1])
        connect(s, s1)
        connect(s, s2)
        connect(e1, e)
        connect(e2, e)
        return s, e

    # Étoile de Kleene
    if node.root == '*':
        s, e = State(), State()
        s1, e1 = tree_to_nfa(node.subtrees[0])
        connect(s, s1)
        connect(s, e)
        connect(e1, s1)
        connect(e1, e)
        return s, e

    return None


class DFAState:
    def __init__(self, nfa_states):
        self.states = list(nfa_states)
        self.transitions = {}
        self.end = any(s.end for s in self.states)


def epsilon_closure(states):
    closure = list(states)
    changed = True
    while changed:
        changed = False
        for s in list(closure):
            for t in s.eps:
                if t not in closure:
                    closure.append(t)
                    changed = True
    return closure


def move(states, c):
    result = []
    for s in states:
        for key in s.edges:
            if key == ANY or key == c:
                result.extend(s.edges[key])
    return result


def collect_states(start):
    visited, stack = set(), [start]
    while stack:
        s = stack.pop()
        if s in visited:
            continue
        visited.add(s)
        for t in s.eps:
            stack.append(t)
        for edges in s.edges.values():
            for t in edges:
                stack.append(t)
    return visited


def nfa_to_dfa(start):
    """Construction d'un DFA par l'algorithme de sous-ensembles."""
    alphabet = set()
    all_states = collect_states(start)
    for s in all_states:
        for c in s.edges:
            alphabet.add(c)
    start_closure = epsilon_closure([start])
    dfa_states = [DFAState(start_closure)]
    queue = [dfa_states[0]]
    while queue:
        current = queue.pop(0)
        for c in alphabet:
            next_states = epsilon_closure(move(current.states, c))
            if not next_states:
                continue
            existing = None
            for d in dfa_states:
                if set(d.states) == set(next_states):
                    existing = d
                    break
            if not existing:
                new_dfa = DFAState(next_states)
                dfa_states.append(new_dfa)
                queue.append(new_dfa)
                existing = new_dfa
            current.transitions[c] = existing
    return dfa_states[0]


def build_dfa_from_regex(pattern):
    """Prépare et compile une regex utilisateur en DFA utilisable pour matcher des mots."""
    regex = check_and_prepare(pattern)
    if regex is None:
        return None
    tree = parse_regex_to_tree(regex)
    s, e = tree_to_nfa(tree)
    e.end = True
    dfa = nfa_to_dfa(s)
    return dfa


def match_dfa_partial(dfa_start, text):
    """
    Matching PARTIEL : la regex peut matcher n'importe quelle partie du texte.
    Retourne True si au moins une sous-chaîne de `text` matche la regex.
    """
    if dfa_start is None:
        return False
    
    # On teste toutes les positions de départ possibles
    for start_pos in range(len(text)):
        current = dfa_start
        
        for char_pos in range(start_pos, len(text)):
            char = text[char_pos]
            next_state = None
            
            # Chercher une transition pour ce caractère
            if char in current.transitions:
                next_state = current.transitions[char]
            elif ANY in current.transitions:
                next_state = current.transitions[ANY]
            
            if next_state is None:
                break  # Plus de transitions possibles depuis cette position
                
            current = next_state
            
            # Si on atteint un état accepteur à n'importe quel moment → match
            if current.end:
                return True
    
    return False


def search_regex_in_index(pattern, index):
    """
    pattern : regex entrée par l'utilisateur
    index   : dict {normalized_word: {book_id: count, ...}, ...}

    Retourne : list of {'id': book_id, 'count': total}
    """
    dfa = build_dfa_from_regex(pattern)
    if dfa is None:
        print(f"❌ Regex invalide: {pattern}")
        return []

    book_counts = {}
    matched_words = []
    
    for word, postings in index.items():
        # ⭐⭐ UTILISE match_dfa_partial au lieu de match_dfa_full ⭐⭐
        if match_dfa_partial(dfa, word):
            matched_words.append(word)
            for book_id, count in postings.items():
                book_counts[book_id] = book_counts.get(book_id, 0) + count

    print(f"📊 Regex '{pattern}' - {len(matched_words)} mots matchés: {matched_words[:10]}...")

    results = [{'id': book_id, 'count': count} for book_id, count in book_counts.items()]
    results.sort(key=lambda x: x['count'], reverse=True)
    return results