type StepFn = (this: Mocha.Context, done: Mocha.Done) => void | Promise<unknown>;
type StepFnNoDone = (this: Mocha.Context) => void | Promise<unknown>;
type Step = (title: string, fn?: StepFn | StepFnNoDone) => Mocha.Test;
type Wrapper = (title: string, fn: (this: Mocha.Suite) => void) => Mocha.Suite;

declare global {
  const expect: Chai.ExpectStatic;
  const Feature: Wrapper;
  const Scenario: Wrapper;
  const Given: Step;
  const When: Step;
  const Then: Step;
  const And: Step;
  const But: Step;
  const beforeEachFeature: (fn: Mocha.Func | Mocha.AsyncFunc) => void;
  const afterEachFeature: (fn: Mocha.Func | Mocha.AsyncFunc) => void;
  const beforeEachScenario: (fn: Mocha.Func | Mocha.AsyncFunc) => void;
  const afterEachScenario: (fn: Mocha.Func | Mocha.AsyncFunc) => void;
}

export {};
